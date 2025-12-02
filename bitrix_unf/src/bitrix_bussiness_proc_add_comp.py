from flask import Flask, request, jsonify
import os
import requests
import json
import csv
from datetime import datetime

BITRIX_WEBHOOK_URL = os.environ.get("BITRIX_WEBHOOK_URL")
ONEC_URL = os.environ.get("ONEC_URL")
ONEC_AUTH_CODE = os.environ.get("ONEC_AUTH_CODE")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV","bp_privязать_компанию_journal.csv")
PORT = int(os.environ.get("PORT","8000"))
if not BITRIX_WEBHOOK_URL or not ONEC_URL or not ONEC_AUTH_CODE:
    raise SystemExit("Required env vars missing")

session = requests.Session()
session.headers.update({"Content-Type":"application/json","Accept":"application/json"})
app = Flask(__name__)

def bitrix_call(method,payload):
    url = BITRIX_WEBHOOK_URL.rstrip("/") + f"/{method}"
    r = session.post(url,json=payload,timeout=30)
    r.raise_for_status()
    return r.json()

def get_deal(deal_id):
    return bitrix_call("crm.deal.get",{"id":deal_id})

def get_company(company_id):
    return bitrix_call("crm.company.get",{"id":company_id})

def update_company(company_id,fields):
    return bitrix_call("crm.company.update",{"id":company_id,"fields":fields})

def list_company_by_bin(bin_):
    return bitrix_call("crm.company.list",{"filter":{"UF_BIN":bin_},"select":["ID","UF_UNF_UUID"]})

def write_log(row):
    header=["timestamp","deal_id","company_id","action","request","response","note"]
    exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV,"a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow(row)

def build_onec_payload(fields_map):
    values=[]
    for k,v in fields_map.items():
        if v:
            values.append({"field":k,"value":v})
    return {"auth_code":ONEC_AUTH_CODE,"values":values}

def extract_company_fields(company):
    f=company.get("result") or {}
    out={}
    out["TITLE"]=f.get("TITLE")
    out["UF_FULL_TITLE"]=f.get("UF_FULL_TITLE")
    out["UF_BIN"]=f.get("UF_BIN")
    out["UF_COMPANY_TYPE"]=f.get("UF_COMPANY_TYPE")
    out["UF_GROUP"]=f.get("UF_GROUP") or f.get("UF_GROUP_ID_VALUE")
    out["ADDRESS"]=f.get("ADDRESS")
    out["EMAIL"]=None
    emails=f.get("EMAIL") or f.get("CONTACTS")
    if isinstance(emails, list) and emails:
        out["EMAIL"]=emails[0]
    out["PHONE"]=None
    phones=f.get("PHONE")
    if isinstance(phones, list) and phones:
        out["PHONE"]=phones[0]
    out["ASSIGNED_BY_ID"]=f.get("ASSIGNED_BY_ID")
    out["COUNTRY"]=f.get("UF_COUNTRY") or f.get("COUNTRY")
    return out

def validate_required(fields):
    req=["TITLE","UF_FULL_TITLE","UF_BIN","UF_COMPANY_TYPE","UF_GROUP"]
    missing=[r for r in req if not fields.get(r)]
    return missing

def onec_post(payload):
    r = session.post(ONEC_URL,json=payload,timeout=30)
    r.raise_for_status()
    return r.json()

@app.route("/webhook",methods=["POST"])
def webhook():
    payload = request.get_json(force=True, silent=True) or {}
    deal_id = None
    if "deal_id" in payload:
        deal_id = payload["deal_id"]
    elif "data" in payload and isinstance(payload["data"],dict):
        deal_id = payload["data"].get("FIELDS") and payload["data"]["FIELDS"].get("ID") or payload["data"].get("ID") or payload["data"].get("FIELDS_ID")
    elif "FIELDS" in payload:
        deal_id = payload.get("FIELDS",{}).get("ID")
    if not deal_id:
        return jsonify({"error":"no deal_id in payload"}),400
    try:
        deal = get_deal(deal_id)
        company_id = (deal.get("result") or {}).get("COMPANY_ID")
        if not company_id:
            write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":"","action":"error","request":json.dumps(deal),"response":"","note":"no company_id"})
            return jsonify({"error":"deal has no company_id"}),200
        comp = get_company(company_id)
        comp_fields = extract_company_fields(comp)
        missing = validate_required(comp_fields)
        if missing:
            write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"validation_failed","request":json.dumps(comp_fields,ensure_ascii=False),"response":"","note":"missing:"+",".join(missing)})
            return jsonify({"status":"error","message":"missing required fields","missing":missing}),200
        map_onec = {
            "customer_name":comp_fields["TITLE"],
            "customer_fullname":comp_fields["UF_FULL_TITLE"],
            "customer_iin_bin":comp_fields["UF_BIN"],
            "customer_type":comp_fields["UF_COMPANY_TYPE"],
            "customer_group_name":comp_fields["UF_GROUP"],
            "customer_address":comp_fields.get("ADDRESS"),
            "customer_email":comp_fields.get("EMAIL"),
            "customer_phone":comp_fields.get("PHONE"),
            "customer_manager_name":comp_fields.get("ASSIGNED_BY_ID"),
            "customer_country_name":comp_fields.get("COUNTRY")
        }
        payload_onec = build_onec_payload(map_onec)
        resp_onec = onec_post(payload_onec)
        write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"post_onec","request":json.dumps(payload_onec,ensure_ascii=False),"response":json.dumps(resp_onec,ensure_ascii=False),"note":""})
        status = resp_onec.get("status")
        if status=="success":
            customer_id = (resp_onec.get("result") or {}).get("customer_id")
            if customer_id:
                upd = {"UF_UNF_UUID":customer_id}
                upd_resp = update_company(company_id,upd)
                write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"set_uuid","request":json.dumps(upd,ensure_ascii=False),"response":json.dumps(upd_resp,ensure_ascii=False),"note":""})
            return jsonify({"status":"ok"}),200
        else:
            msg = resp_onec.get("message","")
            if "Duplicate BIN" in msg or "duplicate" in msg.lower():
                lookup_payload = build_onec_payload({"customer_iin_bin":comp_fields["UF_BIN"]})
                resp_lookup = onec_post(lookup_payload)
                write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"onec_lookup","request":json.dumps(lookup_payload,ensure_ascii=False),"response":json.dumps(resp_lookup,ensure_ascii=False),"note":""})
                if resp_lookup.get("status")=="success" and (resp_lookup.get("result") or {}).get("customer_id"):
                    cid = resp_lookup["result"]["customer_id"]
                    upd = {"UF_UNF_UUID":cid}
                    upd_resp = update_company(company_id,upd)
                    write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"set_uuid_from_lookup","request":json.dumps(upd,ensure_ascii=False),"response":json.dumps(upd_resp,ensure_ascii=False),"note":""})
                    return jsonify({"status":"ok","note":"found_by_bin"}),200
                else:
                    write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"duplicate_bin_no_result","request":json.dumps(lookup_payload,ensure_ascii=False),"response":json.dumps(resp_lookup,ensure_ascii=False),"note":""})
                    return jsonify({"status":"error","message":"duplicate_bin_no_result"}),200
            else:
                write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id,"action":"onec_error","request":json.dumps(payload_onec,ensure_ascii=False),"response":json.dumps(resp_onec,ensure_ascii=False),"note":msg})
                return jsonify({"status":"error","message":msg}),200
    except Exception as e:
        write_log({"timestamp":datetime.utcnow().isoformat(),"deal_id":deal_id,"company_id":company_id if 'company_id' in locals() else "","action":"exception","request":"","response":str(e),"note":""})
        return jsonify({"status":"error","message":str(e)}),500

if __name__=="__main__":
    app.run(host="0.0.0.0",port=PORT)
