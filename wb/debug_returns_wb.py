import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print("=== seller-analytics-api /api/v1/analytics/goods-return ===")

# Пробуем разные наборы параметров
param_variants = [
    {"dateFrom": date_from, "dateTo": date_to},
    {"dateFrom": date_from, "dateTo": date_to, "page": 1},
    {"dateFrom": date_from, "dateTo": date_to, "limit": 10, "offset": 0},
    {"dateFrom": date_from, "dateTo": date_to, "limit": 10, "page": 1},
    {},
]

for params in param_variants:
    r = requests.get(
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-return",
        headers=headers,
        params=params,
        timeout=30,
    )
    print(f"Params: {params}")
    print(f"HTTP: {r.status_code}")
    print(f"Ответ: {r.text[:500]}")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f"Список: {len(data)} записей")
            if data:
                print(f"Ключи первой записи: {list(data[0].keys())}")
                print(f"Первая запись: {json.dumps(data[0], ensure_ascii=False, indent=2)}")
        elif isinstance(data, dict):
            print(f"Ключи верхнего уровня: {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list) and v:
                    print(f"  {k}: {len(v)} записей")
                    print(f"  Ключи первой записи: {list(v[0].keys())}")
                    print(f"  Первая запись: {json.dumps(v[0], ensure_ascii=False, indent=2)}")
                    break
                else:
                    print(f"  {k}: {v}")
        print("=== РАБОТАЕТ ===")
        break
    print()
