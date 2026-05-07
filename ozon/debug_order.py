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

# Пробуем разные периоды для FBO
fbo_periods = [30, 90, 180]

print("=== FBO v2 — структура financial_data ===")
payload = {
    "dir": "DESC",
    "filter": {
        "since": (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": now.strftime("%Y-%m-%dT23:59:59.999Z"),
        "status": "",
    },
    "limit": 1,
    "offset": 0,
    "with": {"analytics_data": False, "financial_data": True},
}
r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json=payload, timeout=30)
data = r.json()
postings = data.get("result", [])
print(f"Статус: {r.status_code}, найдено: {len(postings)}")
if postings:
    print(json.dumps(postings[0].get("financial_data"), indent=2, ensure_ascii=False))
    print("\n=== products ===")
    print(json.dumps(postings[0].get("products"), indent=2, ensure_ascii=False))
