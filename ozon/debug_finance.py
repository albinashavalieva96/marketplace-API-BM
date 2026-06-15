import os
import json
import requests
from datetime import datetime, timedelta, timezone

client_id = os.environ["OZON_BM_CLIENT_ID"]
api_key = os.environ["OZON_BM_API_KEY"]

headers = {
    "Client-Id": client_id,
    "Api-Key": api_key,
    "Content-Type": "application/json",
}

now = datetime.now(timezone.utc)
month = now.strftime("%Y-%m")
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")


def try_endpoint(label, url, body):
    print(f"\n=== {label} ===")
    print(f"URL: {url}")
    r = requests.post(url, headers=headers, json=body, timeout=30)
    print(f"HTTP: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
    else:
        print(f"Ошибка: {r.text[:300]}")


# Транзакции
try_endpoint(
    "v3/finance/transaction/list",
    "https://api-seller.ozon.ru/v3/finance/transaction/list",
    {
        "filter": {
            "date": {"from": date_from, "to": date_to},
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all",
        },
        "page": 1,
        "page_size": 10,
    },
)

try_endpoint(
    "v2/finance/transaction/list",
    "https://api-seller.ozon.ru/v2/finance/transaction/list",
    {
        "filter": {
            "date": {"from": date_from, "to": date_to},
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all",
        },
        "page": 1,
        "page_size": 10,
    },
)

# Реализация — v2 ждёт year+month как числа
try_endpoint(
    "v2/finance/realization (year+month)",
    "https://api-seller.ozon.ru/v2/finance/realization",
    {"year": now.year, "month": now.month},
)

try_endpoint(
    "v2/finance/realization (prev month)",
    "https://api-seller.ozon.ru/v2/finance/realization",
    {"year": (now.replace(day=1) - timedelta(days=1)).year,
     "month": (now.replace(day=1) - timedelta(days=1)).month},
)

# Выплаты / treasury
try_endpoint(
    "v1/finance/treasury/totals",
    "https://api-seller.ozon.ru/v1/finance/treasury/totals",
    {},
)

try_endpoint(
    "v1/finance/treasury/transactions",
    "https://api-seller.ozon.ru/v1/finance/treasury/transactions",
    {"page": 1, "page_size": 10},
)

try_endpoint(
    "v1/finance/cash-flow-statement/list",
    "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list",
    {
        "date": {"from": date_from, "to": date_to},
        "page": 1,
        "page_size": 10,
    },
)
