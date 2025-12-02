import os
import re
import csv
import sys
import time
import json
import logging
from datetime import datetime
import requests
from dateutil import parser as dateparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
AVERS_API_URL = os.environ.get("AVERS_API_URL")
AVERS_API_KEY = os.environ.get("AVERS_API_KEY")
BITRIX_WEBHOOK_URL = os.environ.get("BITRIX_WEBHOOK_URL")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", "integration_journal.csv")
if not AVERS_API_URL or not AVERS_API_KEY or not BITRIX_WEBHOOK_URL:
    logging.error("Required env vars missing: AVERS_API_URL, AVERS_API_KEY, BITRIX_WEBHOOK_URL")
    sys.exit(1)

session = requests.Session()
session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

def avers_get_customers():
    url = AVERS_API_URL.rstrip("/") + "/customers_data"
    params = {"apikey": AVERS_API_KEY}
    resp = session.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and ("data" in data):
        return data["data"]
    return data

def bitrix_call(method, payload):
    url = BITRIX_WEBHOOK_URL.rstrip("/") + f"/{method}"
    resp = session.post(url, json=payload, timeout=60)
    try:
        resp.raise_for_status()
    except Exception as e:
        logging.error("Bitrix call failed %s %s", method, resp.text)
        raise
    return resp.json()

def search_company_by_iin(iin):
    if not iin:
        return []
    filter_ = {"UF_IINBIN": iin}
    result = bitrix_call("crm.company.list", {"filter": filter_, "select": ["ID","UF_UNF_UUID","TITLE","ADDRESS","COMPANY_TYPE","UF_IINBIN"]})
    return result.get("result", []) if isinstance(result, dict) else []

def search_company_by_shortname(name):
    if not name:
        return []
    filter_ = {"TITLE": name}
    result = bitrix_call("crm.company.list", {"filter": filter_, "select": ["ID","UF_UNF_UUID","TITLE","ADDRESS","COMPANY_TYPE","UF_IINBIN"]})
    return result.get("result", []) if isinstance(result, dict) else []

def search_company_by_fullname(name):
    if not name:
        return []
    filter_ = {"HAS_PHONE": False}
    filter_["%TITLE"] = name
    result = bitrix_call("crm.company.list", {"filter": filter_, "select": ["ID","UF_UNF_UUID","TITLE","ADDRESS","COMPANY_TYPE","UF_IINBIN"]})
    return result.get("result", []) if isinstance(result, dict) else []

def create_company(fields):
    payload = {"fields": fields, "params": {"REGISTER_SONET_EVENT": "N"}}
    result = bitrix_call("crm.company.add", payload)
    return result.get("result")

def update_company(company_id, fields):
    payload = {"id": company_id, "fields": fields, "params": {"REGISTER_SONET_EVENT": "N"}}
    result = bitrix_call("crm.company.update", payload)
    return result.get("result")

def add_bank_detail(fields):
    result = bitrix_call("crm.company.bankdetail.add", {"fields": fields})
    return result.get("result")

def parse_bank_string(s):
    if not s or not isinstance(s, str):
        return {}
    patterns = [
        r"(?P<bank>.+?)\s+BIK[:\s]*?(?P<bik>\d{8,9})[,;]?\s*IIK[:\s]*?(?P<iik>[A-Z0-9]{10,34})[,;]?\s*currency[:\s]*?(?P<currency>[A-Z]{3})[,;]?\s*(corr[:\s]*?(?P<corr>[A-Z0-9]{10,34}))?",
        r"(?P<bank>.+?)[,;]\s*BIK[:\s]*?(?P<bik>\d{8,9})[,;]?\s*IIK[:\s]*?(?P<iik>[A-Z0-9]{10,34})[,;]?\s*(?P<currency>[A-Z]{3})?"
    ]
    for p in patterns:
        m = re.search(p, s, re.IGNORECASE)
        if m:
            return {k: (v.strip() if v else None) for k, v in m.groupdict().items()}
    tokens = [t.strip() for t in re.split(r'[;,]', s) if t.strip()]
    out = {}
    for t in tokens:
        if re.search(r"\bBIK\b|\bBIC\b|\b\b\d{8,9}\b", t, re.IGNORECASE):
            bik = re.search(r"(\d{8,9})", t)
            if bik:
                out["bik"] = bik.group(1)
        elif re.search(r"[A-Z0-9]{10,34}", t) and "iik" not in out:
            out["iik"] = re.search(r"([A-Z0-9]{10,34})", t).group(1)
        elif re.search(r"[A-Z]{3}$", t):
            out["currency"] = t[-3:]
        else:
            out.setdefault("bank", t)
    return out

def write_journal_row(row):
    header = ["timestamp","bitrix_company_id","customer_id","action","request_text","response_text","note"]
    exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def safe_json_dump(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except:
        return str(obj)

def map_unf_to_bitrix_fields(c):
    fields = {}
    if c.get("customer_name"):
        fields["TITLE"] = c.get("customer_name")
    if c.get("customer_fullname"):
        fields["UF_FULL_NAME"] = c.get("customer_fullname")
    if c.get("customer_iin_bin"):
        fields["UF_IINBIN"] = c.get("customer_iin_bin")
    if c.get("customer_type"):
        fields["COMPANY_TYPE"] = c.get("customer_type")
    if c.get("customer_group_name"):
        fields["UF_COMPANY_GROUP"] = c.get("customer_group_name")
    if c.get("customer_address"):
        fields["ADDRESS"] = c.get("customer_address")
    if c.get("customer_country_name"):
        fields["UF_COUNTRY"] = c.get("customer_country_name")
    return fields

def process_customer(c):
    customer_id = c.get("customer_code") or c.get("customer_id") or c.get("id")
    iin = c.get("customer_iin_bin")
    short = c.get("customer_name")
    full = c.get("customer_fullname")
    found = []
    if iin:
        found = search_company_by_iin(iin)
    if not found and short:
        found = search_company_by_shortname(short)
    if not found and full:
        found = search_company_by_fullname(full)
    now = datetime.utcnow().isoformat()
    bf = map_unf_to_bitrix_fields(c)
    if found:
        company = found[0]
        company_id = company.get("ID")
        existing_uuid = company.get("UF_UNF_UUID")
        action = "updated"
        if not existing_uuid and customer_id:
            bf["UF_UNF_UUID"] = customer_id
        resp = update_company(company_id, bf)
        write_journal_row({
            "timestamp": now,
            "bitrix_company_id": company_id,
            "customer_id": customer_id,
            "action": action,
            "request_text": safe_json_dump(bf),
            "response_text": safe_json_dump(resp),
            "note": ""
        })
        if c.get("customer_default_bank_account"):
            bd = parse_bank_string(c.get("customer_default_bank_account"))
            if bd:
                bank_fields = {
                    "ENTITY_ID": company_id,
                    "ENTITY_TYPE_ID": "COMPANY",
                    "NAME": bd.get("bank") or "",
                    "SORT": 100,
                    "IS_DEFAULT": "Y",
                    "CODE": "MAIN",
                    "ACCOUNT": bd.get("iik") or "",
                    "BANK_NAME": bd.get("bank") or "",
                    "BANK_BIC": bd.get("bik") or "",
                    "BANK_CORRESPONDENT_ACCOUNT": bd.get("corr") or "",
                    "CURRENCY": bd.get("currency") or ""
                }
                try:
                    res = add_bank_detail(bank_fields)
                except Exception as e:
                    res = {"error": str(e)}
                write_journal_row({
                    "timestamp": now,
                    "bitrix_company_id": company_id,
                    "customer_id": customer_id,
                    "action": "bankdetail_added",
                    "request_text": safe_json_dump(bank_fields),
                    "response_text": safe_json_dump(res),
                    "note": ""
                })
        return "found"
    else:
        if not short and not full and not iin:
            write_journal_row({
                "timestamp": now,
                "bitrix_company_id": "",
                "customer_id": customer_id,
                "action": "skipped",
                "request_text": "",
                "response_text": "",
                "note": "missing key identifiers"
            })
            return "skipped"
        fields = bf.copy()
        if customer_id:
            fields["UF_UNF_UUID"] = customer_id
        try:
            new_id = create_company(fields)
        except Exception as e:
            new_id = None
            write_journal_row({
                "timestamp": now,
                "bitrix_company_id": "",
                "customer_id": customer_id,
                "action": "error_create",
                "request_text": safe_json_dump(fields),
                "response_text": str(e),
                "note": ""
            })
            return "error_create"
        write_journal_row({
            "timestamp": now,
            "bitrix_company_id": new_id,
            "customer_id": customer_id,
            "action": "created",
            "request_text": safe_json_dump(fields),
            "response_text": safe_json_dump({"company_id": new_id}),
            "note": ""
        })
        if c.get("customer_default_bank_account"):
            bd = parse_bank_string(c.get("customer_default_bank_account"))
            if bd:
                bank_fields = {
                    "ENTITY_ID": new_id,
                    "ENTITY_TYPE_ID": "COMPANY",
                    "NAME": bd.get("bank") or "",
                    "SORT": 100,
                    "IS_DEFAULT": "Y",
                    "CODE": "MAIN",
                    "ACCOUNT": bd.get("iik") or "",
                    "BANK_NAME": bd.get("bank") or "",
                    "BANK_BIC": bd.get("bik") or "",
                    "BANK_CORRESPONDENT_ACCOUNT": bd.get("corr") or "",
                    "CURRENCY": bd.get("currency") or ""
                }
                try:
                    res = add_bank_detail(bank_fields)
                except Exception as e:
                    res = {"error": str(e)}
                write_journal_row({
                    "timestamp": now,
                    "bitrix_company_id": new_id,
                    "customer_id": customer_id,
                    "action": "bankdetail_added",
                    "request_text": safe_json_dump(bank_fields),
                    "response_text": safe_json_dump(res),
                    "note": ""
                })
        return "created"

def run_sync():
    customers = avers_get_customers()
    total = len(customers) if isinstance(customers, list) else 0
    stats = {"total": total, "found": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0}
    for c in customers:
        try:
            res = process_customer(c)
            if res == "found":
                stats["found"] += 1
            elif res == "created":
                stats["created"] += 1
            elif res == "skipped":
                stats["skipped"] += 1
            elif res == "error_create":
                stats["errors"] += 1
        except Exception as e:
            logging.exception("Error processing customer")
            stats["errors"] += 1
            write_journal_row({
                "timestamp": datetime.utcnow().isoformat(),
                "bitrix_company_id": "",
                "customer_id": c.get("customer_code") or c.get("customer_id") or "",
                "action": "exception",
                "request_text": safe_json_dump(c),
                "response_text": str(e),
                "note": ""
            })
    report = {
        "total_from_UNF": stats["total"],
        "found_in_Bitrix": stats["found"],
        "created_in_Bitrix": stats["created"],
        "updated": stats["found"],
        "skipped_missing_key": stats["skipped"],
        "errors": stats["errors"]
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    run_sync()
