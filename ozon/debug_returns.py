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
date_from = (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00.000Z")
date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

# ── 1. Перебор версий возвратных эндпоинтов ──────────────────────────────────
print("=== Поиск актуальных эндпоинтов возвратов ===")
candidates = [
    ("POST", "https://api-seller.ozon.ru/v4/returns/company/fbs", {"filter": {}, "limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v5/returns/company/fbs", {"filter": {}, "limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v3/returns/company/fbo", {"filter": {}, "limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v4/returns/company/fbo", {"filter": {}, "limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v1/returns/list",        {"limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v2/returns/list",        {"limit": 5, "offset": 0}),
    ("POST", "https://api-seller.ozon.ru/v1/customer/return/list",{"limit": 5, "offset": 0}),
    ("GET",  "https://api-seller.ozon.ru/v1/returns",             {}),
]
for method, url, body in candidates:
    if method == "POST":
        rr = requests.post(url, headers=headers, json=body, timeout=10)
    else:
        rr = requests.get(url, headers=headers, timeout=10)
    print(f"  {method} {url.split('.ru')[1]:45s} → {rr.status_code}: {rr.text[:120]}")

# ── 2. FBS список — пробуем все возможные статусы возврата ───────────────────
print("\n=== FBS posting/list — статусы возврата ===")
return_statuses = [
    "returned", "return_in_transit", "awaiting_return",
    "client_returned", "cancelled_from_customer",
    "not_accepted", "arbitration", "client_arbitration",
]
for status in return_statuses:
    r = requests.post(
        "https://api-seller.ozon.ru/v3/posting/fbs/list",
        headers=headers,
        json={
            "dir": "DESC",
            "filter": {"since": date_from, "to": date_to, "status": status},
            "limit": 5,
            "offset": 0,
            "with": {"financial_data": False, "analytics_data": False},
        },
        timeout=30,
    )
    postings = r.json().get("result", {}).get("postings", []) if r.status_code == 200 else []
    print(f"  {status:35s} → HTTP {r.status_code}, постингов: {len(postings)}")
    if postings:
        print(f"    пример: {postings[0].get('posting_number')} / {postings[0].get('status')}")

# ── 3. FBO список — статусы возврата ─────────────────────────────────────────
print("\n=== FBO posting/list — статусы возврата ===")
for status in return_statuses:
    r = requests.post(
        "https://api-seller.ozon.ru/v2/posting/fbo/list",
        headers=headers,
        json={
            "dir": "DESC",
            "filter": {"since": date_from, "to": date_to, "status": status},
            "limit": 5,
            "offset": 0,
            "with": {"financial_data": False, "analytics_data": False},
        },
        timeout=30,
    )
    postings = r.json().get("result", []) if r.status_code == 200 else []
    print(f"  {status:35s} → HTTP {r.status_code}, постингов: {len(postings)}")
    if postings:
        print(f"    пример: {postings[0].get('posting_number')} / {postings[0].get('status')}")

# ── 4. Все доставленные FBS за 90 дней — смотрим уникальные статусы ──────────
print("\n=== Уникальные статусы в FBS за 90 дней ===")
r = requests.post(
    "https://api-seller.ozon.ru/v3/posting/fbs/list",
    headers=headers,
    json={
        "dir": "DESC",
        "filter": {"since": date_from, "to": date_to, "status": ""},
        "limit": 100,
        "offset": 0,
        "with": {"financial_data": False, "analytics_data": False},
    },
    timeout=30,
)
if r.status_code == 200:
    postings = r.json().get("result", {}).get("postings", [])
    statuses = {}
    for p in postings:
        s = p.get("status", "")
        statuses[s] = statuses.get(s, 0) + 1
    print(f"  Всего: {len(postings)}, уникальных статусов: {len(statuses)}")
    for s, cnt in sorted(statuses.items(), key=lambda x: x[1], reverse=True):
        print(f"    {s}: {cnt}")
