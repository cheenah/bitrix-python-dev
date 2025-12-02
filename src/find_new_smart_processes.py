from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

from bitrix_utils import fetch_all_smart_processes, get_webhook_from_args_env_secret

def _parse_possible_date(d: dict) -> datetime | None:
    for k in ('DATE_CREATE', 'DATE_INSERT', 'dateCreate', 'created', 'DATE_CREATED'):
        v = d.get(k)
        if not v:
            continue
        if isinstance(v, (int, float)):
            try:
                return datetime.fromtimestamp(int(v), tz=timezone.utc)
            except Exception:
                continue
        if isinstance(v, str):
            s = v.strip()
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            try:
                return datetime.fromisoformat(s)
            except Exception:
                try:
                    return datetime.strptime(s.split('T')[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                except Exception:
                    continue
    return None


def _collect_ids(lst: List[dict]) -> List[str]:
    ids = []
    for it in lst:
        if not isinstance(it, dict):
            continue
        iid = it.get('ID') or it.get('id')
        if iid is not None:
            ids.append(str(iid))
    return ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--webhook', '-w')
    parser.add_argument('--secret-name', '-s')
    parser.add_argument('--project-id', '-p')
    parser.add_argument('--out', default='smart_processes.json')
    parser.add_argument('--new-out', default='new_smart_processes.json')
    parser.add_argument('--since-hours', type=int, default=24, help='Only consider items created within last N hours (if creation date present)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    webhook = get_webhook_from_args_env_secret(args.webhook, args.secret_name, args.project_id)
    logging.info('Fetching all smart-processes...')
    items = fetch_all_smart_processes(webhook)
    logging.info('Fetched %d smart-processes', len(items))

    existing = []
    if os.path.exists(args.out):
        try:
            existing = json.load(open(args.out, encoding='utf-8'))
        except Exception:
            existing = []

    ids_old = set(_collect_ids(existing))
    ids_new = _collect_ids(items)
    new_items = [it for it in items if (str(it.get('ID') or it.get('id')) not in ids_old)]
    if args.since_hours and args.since_hours > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        new_recent = []
        for it in new_items:
            dt = _parse_possible_date(it)
            if dt is None:
    
                new_recent.append(it)
            elif dt >= cutoff:
                new_recent.append(it)
        new_items = new_recent

    if not new_items:
        logging.info('Тhe smart-processes list have not changed — file not updated')
        return
    json.dump(new_items, open(args.new_out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    logging.info('Saved %d new smart-processes to %s', len(new_items), args.new_out)
    merged = {str(it.get('ID') or it.get('id')): it for it in existing if isinstance(it, dict)}
    for it in items:
        iid = str(it.get('ID') or it.get('id')) if isinstance(it, dict) and (it.get('ID') or it.get('id')) is not None else None
        if iid:
            merged[iid] = it
    all_merged = list(merged.values())
    json.dump(all_merged, open(args.out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    logging.info('Updated master file %s with %d total items', args.out, len(all_merged))


if __name__ == '__main__':
    main()
