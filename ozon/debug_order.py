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
        "since": (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00.000Z"),
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

response = requests.post(
    "https://api-seller.ozon.ru/v3/posting/fbo/list",
    headers=headers,
    json=payload,
    timeout=30,
)

data = response.json()
postings = data.get("result", {}).get("postings", [])

if postings:
    posting = postings[0]
    print("=== financial_data ===")
    print(json.dumps(posting.get("financial_data"), indent=2, ensure_ascii=False))
    print("\n=== products ===")
    print(json.dumps(posting.get("products"), indent=2, ensure_ascii=False))
else:
    print("Нет заказов за последние 7 дней")
