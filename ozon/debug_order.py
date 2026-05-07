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
payload = {
    "dir": "DESC",
    "filter": {
        "since": (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": now.strftime("%Y-%m-%dT23:59:59.999Z"),
        "status": "",
    },
    "limit": 1,
    "offset": 0,
    "with": {
        "analytics_data": True,
        "financial_data": True,
    },
}

found = False
for schema in ["fbo", "fbs"]:
    response = requests.post(
        f"https://api-seller.ozon.ru/v3/posting/{schema}/list",
        headers=headers,
        json=payload,
        timeout=30,
    )
    data = response.json()
    postings = data.get("result", {}).get("postings", [])

    if postings:
        posting = postings[0]
        print(f"=== {schema.upper()} — FULL POSTING (все поля) ===")
        print(json.dumps(posting, indent=2, ensure_ascii=False))
        found = True
        break

if not found:
    print("Нет заказов за последние 30 дней")
