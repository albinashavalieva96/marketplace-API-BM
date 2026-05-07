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

print("=== Проверка FBO за разные периоды ===")
for days in fbo_periods:
    payload = {
        "dir": "DESC",
        "filter": {
            "since": (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000Z"),
            "to": now.strftime("%Y-%m-%dT23:59:59.999Z"),
            "status": "",
        },
        "limit": 10,
        "offset": 0,
        "with": {"analytics_data": False, "financial_data": True},
    }
    response = requests.post(
        "https://api-seller.ozon.ru/v3/posting/fbo/list",
        headers=headers, json=payload, timeout=30,
    )
    data = response.json()
    postings = data.get("result", {}).get("postings", [])
    print(f"За {days} дней: {response.status_code}, найдено: {len(postings)}")
