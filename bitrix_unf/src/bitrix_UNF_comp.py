import requests
import json
from datetime import datetime

BITRIX_WEBHOOK = "https://your_bitrix/rest/1/xxx/"
UNF_URL = "https://1c.kzt.avers.kz/api/customers_data"
UNF_AUTH = "SECRET"

log = []
stats = {"total":0,"found":0,"created":0,"updated":0,"skipped":0}

def b_call(method, params):
    return requests.post(BITRIX_WEBHOOK + method, json=params).json()

def get_unf_data():
    data = {"auth_code": UNF_AUTH, "values":[{"field":"get_all","value":"1"}]}
    r = requests.post(UNF_URL, json=data).json()
    return r.get("result", [])

def find_b24_company(rec):
    binv = rec["customer_iin_bin"]
    if binv:
        r = b_call("crm.company.list", {"filter":{"UF_IINBIN":binv}})
        if r["result"]:
            return r["result"][0]

    r = b_call("crm.company.list", {"filter":{"TITLE":rec["customer_name"]}})
    if r["result"]:
        return r["result"][0]

    r = b_call("crm.company.list", {"filter":{"UF_FULL_TITLE":rec["customer_fullname"]}})
    if r["result"]:
        return r["result"][0]

    return None

def create_bankdetail(company_id, bank_string):
    parts = bank_string.split(";")
    d = {
        "ENTITY_TYPE_ID": 4,
        "ENTITY_ID": company_id,
        "NAME": parts[0] if len(parts)>0 else "",
        "RQ_BANK_NAME": parts[0] if len(parts)>0 else "",
        "RQ_BIK": parts[1] if len(parts)>1 else "",
        "RQ_ACC_NUM": parts[2] if len(parts)>2 else "",
        "RQ_IBAN": parts[2] if len(parts)>2 else "",
        "RQ_BANK_ADDR": "",
        "RQ_COR_ACC_NUM": parts[4] if len(parts)>4 else "",
    }
    return b_call("crm.requisite.bankdetail.add", {"fields": d})

def update_company(company_id, rec):
    fields = {
        "TITLE": rec["customer_name"],
        "UF_FULL_TITLE": rec["customer_fullname"],
        "UF_IINBIN": rec["customer_iin_bin"],
        "UF_COMPANY_TYPE": rec["customer_type"],
        "UF_GROUP_ID_VALUE": rec["customer_group_name"],
        "ADDRESS": rec["customer_address"],
        "UF_COUNTRY": rec["customer_country_name"]
    }
    return b_call("crm.company.update", {"id": company_id, "fields": fields})

def create_company(rec):
    fields = {
        "TITLE": rec["customer_name"],
        "UF_FULL_TITLE": rec["customer_fullname"],
        "UF_IINBIN": rec["customer_iin_bin"],
        "UF_COMPANY_TYPE": rec["customer_type"],
        "UF_GROUP_ID_VALUE": rec["customer_group_name"],
        "ADDRESS": rec["customer_address"],
        "UF_COUNTRY": rec["customer_country_name"],
        "UF_UNF_UUID": rec["customer_code"]
    }
    r = b_call("crm.company.add", {"fields": fields})
    if "result" in r:
        return r["result"]
    return None

def process():
    data = get_unf_data()
    stats["total"] = len(data)

    for rec in data:
        if not rec["customer_name"] or not rec["customer_fullname"]:
            stats["skipped"] += 1
            continue

        found = find_b24_company(rec)

        if found:
            stats["found"] += 1

            if not found.get("UF_UNF_UUID"):
                b_call("crm.company.update", {
                    "id": found["ID"],
                    "fields": {"UF_UNF_UUID": rec["customer_code"]}
                })

            update_company(found["ID"], rec)
            stats["updated"] += 1

            if rec["customer_default_bank_account"]:
                create_bankdetail(found["ID"], rec["customer_default_bank_account"])

            log.append({
                "action":"updated",
                "b24_id": found["ID"],
                "customer_id": rec["customer_code"],
                "time": str(datetime.now())
            })
        else:
            new_id = create_company(rec)
            stats["created"] += 1

            if rec["customer_default_bank_account"]:
                create_bankdetail(new_id, rec["customer_default_bank_account"])

            log.append({
                "action":"created",
                "b24_id": new_id,
                "customer_id": rec["customer_code"],
                "time": str(datetime.now())
            })

    return {"stats":stats,"log":log}

result = process()
print(json.dumps(result, ensure_ascii=False, indent=2))
