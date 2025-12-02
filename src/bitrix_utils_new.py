from __future__ import annotations

import json
import logging
import os
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOG_FILE = "errors.log"
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


def _process_uf_field(value: Any, field_info: Dict[str, Any]) -> Any:
    """Обработка пользовательского поля в зависимости от его типа"""
    field_type = field_info.get('type')
    
    if field_type == 'enumeration' and isinstance(value, (list, str, int)):
        # Для списков значений
        if isinstance(value, (str, int)):
            value = [value]
        processed_value = []
        for v in value:
            if isinstance(v, dict):
                processed_value.append(v)
            else:
                enum_info = field_info.get('items', {}).get(str(v), {})
                processed_value.append({
                    'id': v,
                    'title': enum_info.get('title', str(v))
                })
        return processed_value
    
    elif field_type == 'crm_status' and value:
        # Для статусов CRM
        if isinstance(value, dict):
            return value
        return {
            'id': value,
            'title': field_info.get('items', {}).get(str(value), {}).get('title', str(value))
        }
    
    elif field_type == 'crm' and value:
        # Для связей с CRM сущностями
        if isinstance(value, dict):
            return value
        return {
            'id': value,
            'title': field_info.get('items', {}).get(str(value), {}).get('title', str(value))
        }
    
    elif field_type == 'file' and value:
        # Для файлов
        if isinstance(value, dict):
            return value
        return {
            'id': value,
            'fileId': value
        }
    
    elif field_type == 'boolean' and value:
        # Для булевых значений
        return value == 'Y' or value is True
    
    return value


def fetch_items_for_smart_process(webhook_base_url: str, type_id: int | str, timeout: int = 15) -> List[dict]:
    webhook_base_url = webhook_base_url.rstrip('/')
    
    # Сначала получим описание полей смарт-процесса
    fields_endpoint = urljoin(webhook_base_url + '/', 'crm.item.fields')
    session = _make_session()
    try:
        fields_data = _post(session, fields_endpoint, {'entityTypeId': int(type_id)}, timeout=timeout)
        fields_info = fields_data.get('result', {})
    except Exception as e:
        error_logger.error('Failed to fetch fields info: %s', e)
        fields_info = {}

    endpoint = urljoin(webhook_base_url + '/', 'crm.item.list')
    all_items: List[dict] = []
    start = 0

    while True:
        payload = {'start': start, 'entityTypeId': int(type_id)}
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

        # Обработка UF-полей для каждого элемента
        processed_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            
            processed_item = {}
            for key, value in item.items():
                # Обработка UF-полей
                if key.startswith('uf_') or key.startswith('UF_'):
                    field_info = fields_info.get(key, {})
                    processed_item[key] = _process_uf_field(value, field_info)
                else:
                    processed_item[key] = value
            
            processed_items.append(processed_item)

        all_items.extend(processed_items)
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