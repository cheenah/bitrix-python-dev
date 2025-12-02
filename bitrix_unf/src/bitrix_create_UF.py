import requests

WEBHOOK_URL = "https://b24-59kuce.bitrix24.kz/rest/1/rdzqmgb8i6sa0vac/"

def add_field(data):
    r = requests.post(WEBHOOK_URL + "crm.company.userfield.add.json", json=data)
    print(r.json())

add_field({
    "fields": {
        "FIELD_NAME": "Полное название",
        "EDIT_FORM_LABEL": "Полное название",
        "LIST_COLUMN_LABEL": "Полное название",
        "USER_TYPE_ID": "string",
        "FIELD_CODE": "UF_FULL_TITLE"
    }
})

add_field({
    "fields": {
        "FIELD_NAME": "БИН ИИН",
        "EDIT_FORM_LABEL": "БИН ИИН",
        "LIST_COLUMN_LABEL": "БИН ИИН",
        "USER_TYPE_ID": "string",
        "FIELD_CODE": "UF_BIN"
    }
})

add_field({
    "fields": {
        "FIELD_NAME": "Тип компании",
        "EDIT_FORM_LABEL": "Тип компании",
        "LIST_COLUMN_LABEL": "Тип компании",
        "USER_TYPE_ID": "enumeration",
        "FIELD_CODE": "UF_COMPANY_TYPE",
        "LIST": [
            {"VALUE": "Юр.лицо"},
            {"VALUE": "Физ.лицо"}
        ]
    }
})

add_field({
    "fields": {
        "FIELD_NAME": "Группа клиента",
        "EDIT_FORM_LABEL": "Группа клиента",
        "LIST_COLUMN_LABEL": "Группа клиента",
        "USER_TYPE_ID": "string",
        "FIELD_CODE": "UF_GROUP_ID_VALUE"
    }
})

add_field({
    "fields": {
        "FIELD_NAME": "UUID 1С",
        "EDIT_FORM_LABEL": "UUID 1С",
        "LIST_COLUMN_LABEL": "UUID 1С",
        "USER_TYPE_ID": "string",
        "FIELD_CODE": "UF_UNF_UUID"
    }
})
