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

for schema in ["fbo", "fbs"]:
    response = requests.post(
        f"https://api-seller.ozon.ru/v3/posting/{schema}/list",
        headers=headers,
        json=payload,
        timeout=30,
    )
    print(f"\n=== {schema.upper()} — статус ответа: {response.status_code} ===")
    data = response.json()

    if response.status_code != 200:
        print(f"Ошибка: {data}")
        continue

    postings = data.get("result", {}).get("postings", [])
    print(f"Найдено отправлений: {len(postings)}")

    if postings:
        print(f"Первое отправление:")
        print(json.dumps(postings[0], indent=2, ensure_ascii=False))
