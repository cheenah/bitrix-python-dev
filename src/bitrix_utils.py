from __future__ import annotations

import json
import logging
import os
from typing import List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

    
LOG_FILE = "../errors.log"
error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
error_logger.addHandler(file_handler)
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def _make_session(retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def _fetch_secret_from_gcp(project_id: str, secret_name: str) -> str:
    try:
        from google.cloud import secretmanager
    except Exception as e:  
        raise RuntimeError('google-cloud-secret-manager package required: ' + str(e))

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('utf-8')


def get_webhook_from_args_env_secret(webhook: Optional[str], secret_name: Optional[str], project_id: Optional[str]) -> str:
    if webhook:
        return webhook.strip()

    if secret_name:
        if not project_id:
            raise SystemExit(2)
        try:
            return _fetch_secret_from_gcp(project_id, secret_name).strip()
        except Exception as e:
            error_logger.error('Failed to read secret from GCP: %s', e)
            raise SystemExit(3)

    env = os.environ.get('WEBHOOK')
    if env:
        return env.strip()

    raise SystemExit(1)


def _post(session: requests.Session, endpoint: str, payload: dict, timeout: int = 15) -> dict:
    resp = session.post(endpoint, json=payload, timeout=timeout)
    if not resp.ok:
        error_logger.error(
            "HTTP %s for %s\nPayload: %s\nResponse: %s",
            resp.status_code,
            endpoint,
            json.dumps(payload),
            resp.text[:500],
        )
    resp.raise_for_status()
    return resp.json()


def fetch_all_smart_processes(webhook_base_url: str, timeout: int = 15) -> List[dict]:
    webhook_base_url = webhook_base_url.rstrip('/')
    endpoint = urljoin(webhook_base_url + '/', 'crm.type.list') if not webhook_base_url.endswith('crm.type.list') else webhook_base_url

    session = _make_session()
    all_items: List[dict] = []
    start = 0

    while True:
        payload = {'start': start}
        try:
            data = _post(session, endpoint, payload, timeout=timeout)
        except requests.exceptions.RequestException as e:
            error_logger.error('HTTP request failed: %s', e)
            raise RuntimeError(f'HTTP request failed: {e}')

        if isinstance(data, dict) and data.get('error'):
            error_logger.error('API error: %s', str(data.get('error_description') or data.get('error')))
            raise RuntimeError('API error: ' + str(data.get('error_description') or data.get('error')))

        result = data.get('result') if isinstance(data, dict) else None
        if result is None:
            if isinstance(data, list):
                items = [it for it in data if isinstance(it, dict)]
            else:
                raise RuntimeError('Unexpected API response structure')
        else:
            items = result.get('types') or result.get('list') or result.get('items') or result.get('result') or result
            if isinstance(items, dict) and 'items' in items:
                items = items['items']
            if not isinstance(items, list):
                possible = []
                if isinstance(result, dict):
                    for v in result.values():
                        if isinstance(v, list):
                            possible = v
                            break
                items = possible

        if not items:
            break

        all_items.extend([it for it in items if isinstance(it, dict)])
        next_start = None
        if isinstance(result, dict):
            for key in ('next', 'next_start', 'start'):
                if key in result:
                    try:
                        ns = int(result[key])
                        if ns and ns != start:
                            next_start = ns
                            break
                    except Exception:
                        pass
            if next_start is None and isinstance(result.get('more'), bool) and result.get('more') is True:
                next_start = start + len(items)

        if next_start is None:
            if len(items) < 50:
                break
            start += len(items)
        else:
            start = next_start

    return all_items


def fetch_items_for_smart_process(webhook_base_url: str, type_id: int | str, timeout: int = 15) -> List[dict]:
    webhook_base_url = webhook_base_url.rstrip('/')
    endpoint = urljoin(webhook_base_url + '/', 'crm.item.list')

    session = _make_session()
    all_items: List[dict] = []
    start = 0

    fields_data = {'result': {}}
    fields_endpoint = urljoin(webhook_base_url + '/', 'crm.item.fields')
    try:
        fields_data = _post(session, fields_endpoint, {'entityTypeId': int(type_id)}, timeout=timeout)
    except Exception as e:
        error_logger.error('Failed to fetch field metadata for %s: %s', type_id, e)

    fields_map = fields_data.get('result') or {}
    if isinstance(fields_map.get('fields'), dict):
        fields_map = fields_map['fields']

    def _get_field_info(key: str):
        meta = None
        for k in fields_map.keys():
            if k.lower() == key.lower():
                meta = fields_map[k]
                break
        meta = meta or {}
        return meta

    def _augment_uf_value(key: str, val):
        meta = _get_field_info(key)
        field_info = {
            'title': meta.get('title', ''),
            'type': meta.get('type', ''),
            'isRequired': meta.get('isRequired', ''),
            'isMultiple': meta.get('isMultiple', ''),
            'settings': meta.get('settings', {}),
            'items': meta.get('items', []),
            'value': val
        }

        if field_info['type'] == 'enumeration' and field_info['items']:
            id_to_title = {it['ID']: it['VALUE'] for it in field_info['items']}
            if isinstance(val, list):
                field_info['titles'] = [id_to_title.get(str(v), str(v)) for v in val]
            else:
                field_info['titleValue'] = id_to_title.get(str(val), str(val))

        return field_info

    all_uf_keys = [k for k in fields_map.keys() if k.upper().startswith('UF_')]

    while True:
        payload = {'start': start, 'entityTypeId': int(type_id), 'select': ['*', 'UF_*']}
        try:
            data = _post(session, endpoint, payload, timeout=timeout)
        except requests.exceptions.RequestException as e:
            error_logger.error('HTTP request failed: %s', e)
            raise RuntimeError(f'HTTP request failed: {e}')

        if isinstance(data, dict) and data.get('error'):
            error_logger.error('API error: %s', str(data.get('error_description') or data.get('error')))
            raise RuntimeError('API error: ' + str(data.get('error_description') or data.get('error')))

        result = data.get('result') if isinstance(data, dict) else None
        if result is None:
            if isinstance(data, list):
                items = [it for it in data if isinstance(it, dict)]
            else:
                raise RuntimeError('Unexpected API response structure')
        else:
            items = result.get('items') or result.get('list') or result.get('result') or result
            if isinstance(items, dict) and 'items' in items:
                items = items['items']
            if not isinstance(items, list):
                possible = []
                if isinstance(result, dict):
                    for v in result.values():
                        if isinstance(v, list):
                            possible = v
                            break
                items = possible

        if not items:
            break

        for it in items:
            if not isinstance(it, dict):
                continue

            for uf_key in all_uf_keys:
                if uf_key not in it:
                    meta = _get_field_info(uf_key)
                    it[uf_key] = {
                        'value': None,
                        'title': meta.get('title', ''),
                        'type': meta.get('type', ''),
                        'isRequired': meta.get('isRequired', ''),
                        'isMultiple': meta.get('isMultiple', ''),
                        'settings': meta.get('settings', {}),
                        'items': meta.get('items', [])
                    }

            for key in list(it.keys()):
                if key.lower().startswith('uf'):
                    try:
                        it[key] = _augment_uf_value(key, it.get(key))
                    except Exception as e:
                        error_logger.error('Failed to augment field %s for type %s: %s', key, type_id, e)

        all_items.extend([it for it in items if isinstance(it, dict)])

        next_start = None
        if isinstance(result, dict):
            for key in ('next', 'next_start', 'start'):
                if key in result:
                    try:
                        ns = int(result[key])
                        if ns and ns != start:
                            next_start = ns
                            break
                    except Exception:
                        pass
            if next_start is None and isinstance(result.get('more'), bool) and result.get('more') is True:
                next_start = start + len(items)

        if next_start is None:
            if len(items) < 50:
                break
            start += len(items)
        else:
            start = next_start

    return all_items

