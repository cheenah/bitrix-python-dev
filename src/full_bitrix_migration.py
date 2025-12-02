import json
import time
import sqlite3
import requests

OLD_BX = "https://avers.bitrix24.kz/rest/3173/8v1zm7w92acl6ofj/"
NEW_BX = "https://b24-59kuce.bitrix24.kz/rest/1/02tqgcjofh989jja/"
DB = "migration_log.db"
TXT = "migration_output.txt"
SLEEP = 0.2
TIMEOUT = 30

def out(s):
    print(s)
    with open(TXT, "a", encoding="utf-8") as f:
        f.write(s + "\n")

def db_conn():
    c = sqlite3.connect(DB, isolation_level=None)
    c.execute("CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, old_id TEXT, new_id TEXT, action TEXT, req TEXT, resp TEXT, ts TEXT)")
    return c

def log(c, old_id, new_id, action, req, resp):
    c.execute("INSERT INTO log (old_id,new_id,action,req,resp,ts) VALUES (?,?,?,?,?,datetime('now'))",
              (str(old_id),str(new_id),action,json.dumps(req,ensure_ascii=False),json.dumps(resp,ensure_ascii=False)))

def bx(method, params, url):
    r = requests.post(url.rstrip("/")+"/"+method, json=params, timeout=TIMEOUT)
    try:
        return r.json()
    except:
        return {"error": "invalid_response", "data": r.text}

def get_all_companies(url):
    res=[]
    start=0
    while True:
        r=bx("crm.company.list", {"start": start, "select": ["*","UF_*"]}, url)
        items=r.get("result",[])
        if not items: break
        res.extend(items)
        if "next" not in r: break
        start=r["next"]
        time.sleep(SLEEP)
    return res

def get_requisites(company_id, url):
    r=bx("crm.requisite.list", {"filter":{"ENTITY_ID":company_id,"ENTITY_TYPE_ID":4}}, url)
    return r.get("result",[])

def get_bank_details(req_id, url):
    r=bx("crm.requisite.bankdetail.list", {"filter":{"ENTITY_ID":req_id}}, url)
    return r.get("result",[])

def create_company(fields, url):
    return bx("crm.company.add", {"fields": fields}, url)

def update_company(cid, fields, url):
    return bx("crm.company.update", {"id": int(cid), "fields": fields}, url)

def create_requisite(fields, url):
    return bx("crm.requisite.add", {"fields": fields}, url)

def create_bank(fields, url):
    return bx("crm.requisite.bankdetail.add", {"fields": fields}, url)

def search_new_company(old):
    iin=old.get("UF_IINBIN")
    full=old.get("COMPANY_FULL_NAME")
    short=old.get("TITLE")
    if iin:
        r=bx("crm.company.list", {"filter": {"UF_IINBIN": iin}}, NEW_BX)
        if r.get("result"): return r["result"][0]
    if short:
        r=bx("crm.company.list", {"filter": {"TITLE": short}}, NEW_BX)
        if r.get("result"): return r["result"][0]
    if full:
        r=bx("crm.company.list", {"filter": {"COMPANY_FULL_NAME": full}}, NEW_BX)
        if r.get("result"): return r["result"][0]
    return None

def prepare_company_fields(old):
    f={}
    for k,v in old.items():
        if v is None: continue
        if k in ("ID","DATE_CREATE","DATE_MODIFY"): continue
        if k.startswith("ORIGIN_"): continue
        f[k]=v
    return f

def prepare_requisite_fields(rq, new_company_id):
    f={}
    for k,v in rq.items():
        if k in ("ID","ENTITY_ID","PRESET_ID","DATE_CREATE","DATE_MODIFY"): continue
        f[k]=v
    f["ENTITY_ID"]=new_company_id
    f["ENTITY_TYPE_ID"]=4
    return f

def prepare_bank_fields(bd, new_req_id):
    f={}
    for k,v in bd.items():
        if k in ("ID","ENTITY_ID","DATE_CREATE","DATE_MODIFY"): continue
        f[k]=v
    f["ENTITY_ID"]=new_req_id
    return f

def migrate():
    conn=db_conn()
    out("Загрузка компаний из старого портала...")
    old_list=get_all_companies(OLD_BX)
    out(f"Найдено компаний: {len(old_list)}")

    for old in old_list:
        old_id=old.get("ID")
        short=old.get("TITLE") or ""
        out(f"--- Компания OLD_ID {old_id}: {short}")

        found=search_new_company(old)
        fields=prepare_company_fields(old)

        if found:
            new_id=found.get("ID")
            out(f"Найдена в новом портале NEW_ID {new_id}. Обновление...")
            r=update_company(new_id, fields, NEW_BX)
            log(conn, old_id, new_id, "update_company", fields, r)
            out(f"Обновлено: {json.dumps(r,ensure_ascii=False)}")
        else:
            out("Не найдена. Создание...")
            r=create_company(fields, NEW_BX)
            new_id=r.get("result")
            log(conn, old_id, new_id, "create_company", fields, r)
            out(f"Создана NEW_ID {new_id}")
            if not new_id:
                time.sleep(SLEEP)
                continue

        reqs=get_requisites(old_id, OLD_BX)
        out(f"Реквизитов: {len(reqs)}")

        for rq in reqs:
            rq_fields=prepare_requisite_fields(rq, new_id)
            r=create_requisite(rq_fields, NEW_BX)
            new_req_id=r.get("result")
            log(conn, old_id, new_req_id, "create_requisite", rq_fields, r)
            out(f"Создан реквизит ID={new_req_id}")

            if not new_req_id:
                time.sleep(SLEEP)
                continue

            banks=get_bank_details(rq.get("ID"), OLD_BX)
            out(f"Банковских реквизитов: {len(banks)}")

            for bd in banks:
                bd_fields=prepare_bank_fields(bd, new_req_id)
                r=create_bank(bd_fields, NEW_BX)
                log(conn, old_id, new_req_id, "create_bank", bd_fields, r)
                out(f"Создан банк-реквизит: {json.dumps(r,ensure_ascii=False)}")

        time.sleep(SLEEP)

    out("Миграция завершена.")

if __name__ == "__main__":
    migrate()
