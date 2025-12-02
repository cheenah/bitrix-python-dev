from __future__ import annotations
import argparse
import json
import logging
import os
from typing import List
from pathlib import Path
import pandas as pd

from bitrix_utils import fetch_items_for_smart_process, get_webhook_from_args_env_secret


def _extract_type_ids_from_file(path: str) -> List[str]:
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return []
    ids = []
    for it in data:
        if isinstance(it, dict):
            entity_type_id = it.get("entityTypeId")
            if entity_type_id is not None:
                ids.append(str(entity_type_id))
    return ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--webhook", "-w")
    parser.add_argument("--secret-name", "-s")
    parser.add_argument("--project-id", "-p")
    parser.add_argument("--type-ids", help="Comma-separated type IDs")
    parser.add_argument("--in-file", help="Read type IDs from smart_processes.json")
    parser.add_argument("--out-dir", default=".", help="Output directory for JSON and Parquet files")
    parser.add_argument("--timeout", type=int, default=15)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    webhook = get_webhook_from_args_env_secret(args.webhook, args.secret_name, args.project_id)

    type_ids: List[str] = []
    if args.type_ids:
        type_ids = [s.strip() for s in args.type_ids.split(",") if s.strip()]
    if args.in_file:
        type_ids_from_file = _extract_type_ids_from_file(args.in_file)
        type_ids = type_ids or type_ids_from_file

    if not type_ids:
        logging.error("No type IDs provided via --type-ids or --in-file")
        return

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    all_data = []  

    for tid in type_ids:
        logging.info("Fetching items for smart-process type %s...", tid)
        try:
            items = fetch_items_for_smart_process(webhook, tid, timeout=args.timeout)
        except Exception as e:
            logging.error("Failed to fetch items for %s: %s", tid, e)
            continue

        type_dir = Path(args.out_dir) / f"type_{tid}"
        type_dir.mkdir(parents=True, exist_ok=True)

        new_items = []
        for i, item in enumerate(items, start=1):
            item_id = item.get("id") or item.get("ID") or i
            out_path = type_dir / f"item_{item_id}.json"
            if out_path.exists():
                continue  
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(item, f, ensure_ascii=False, indent=2)
            item["type_id"] = tid
            new_items.append(item)
            all_data.append(item)

        logging.info("Added %d new items for type %s", len(new_items), tid)

        if new_items:
            df_new = pd.json_normalize(new_items)
            parquet_path = type_dir / f"type_{tid}.parquet"
            if parquet_path.exists():
                df_old = pd.read_parquet(parquet_path)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_all = df_new
            df_all = df_all.astype(str)
            df_all.to_parquet(parquet_path, index=False)

            logging.info("Updated parquet file: %s (%d total rows)", parquet_path.name, len(df_all))

    logging.info("All types processed successfully.")


if __name__ == "__main__":
    main()
