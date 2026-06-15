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
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
prev = now.replace(day=1) - timedelta(days=1)

# 1. Реализация прошлого месяца — полная первая строка + заголовок
print("\n=== v2/finance/realization (prev month) — ПОЛНЫЙ ОТВЕТ ===")
r = requests.post(
    "https://api-seller.ozon.ru/v2/finance/realization",
    headers=headers,
    json={"year": prev.year, "month": prev.month},
    timeout=30,
)
print(f"HTTP: {r.status_code}")
if r.status_code == 200:
    data = r.json().get("result", {})
    print("--- header ---")
    print(json.dumps(data.get("header", {}), indent=2, ensure_ascii=False))
    rows = data.get("rows", [])
    print(f"\nВсего строк: {len(rows)}")
    if rows:
        print("--- первая строка (полностью) ---")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))
    totals = data.get("totals", {})
    print("--- totals ---")
    print(json.dumps(totals, indent=2, ensure_ascii=False))
else:
    print(r.text[:500])

# 2. Транзакции — одна запись полностью
print("\n=== v3/finance/transaction/list — первая операция полностью ===")
r = requests.post(
    "https://api-seller.ozon.ru/v3/finance/transaction/list",
    headers=headers,
    json={
        "filter": {
            "date": {"from": date_from, "to": date_to},
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all",
        },
        "page": 1,
        "page_size": 1,
    },
    timeout=30,
)
print(f"HTTP: {r.status_code}")
if r.status_code == 200:
    ops = r.json().get("result", {}).get("operations", [])
    if ops:
        print(json.dumps(ops[0], indent=2, ensure_ascii=False))
    print(f"Всего операций в периоде: {r.json().get('result', {}).get('page_count', '?')}")
else:
    print(r.text[:300])

# 3. Cash-flow — все периоды
print("\n=== v1/finance/cash-flow-statement/list — все периоды ===")
r = requests.post(
    "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list",
    headers=headers,
    json={
        "date": {"from": date_from, "to": date_to},
        "page": 1,
        "page_size": 20,
    },
    timeout=30,
)
print(f"HTTP: {r.status_code}")
if r.status_code == 200:
    flows = r.json().get("result", {}).get("cash_flows", [])
    for f in flows:
        net = (f.get("orders_amount", 0) + f.get("returns_amount", 0)
               + f.get("commission_amount", 0) + f.get("services_amount", 0)
               + f.get("item_delivery_and_return_amount", 0))
        begin = f["period"]["begin"][:10]
        end = f["period"]["end"][:10]
        print(f"  {begin} – {end}: NET = {round(net, 2):>12} ₽  "
              f"(заказы {f.get('orders_amount',0)}, возвраты {f.get('returns_amount',0)}, "
              f"комиссия {f.get('commission_amount',0)}, логистика {f.get('item_delivery_and_return_amount',0)})")
else:
    print(r.text[:300])
