import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")

candidates = [
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/returns", {"limit": 5, "next": 0}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v1/returns", {"limit": 5, "next": 0}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/returns/orders", {"limit": 5}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/returns/goods", {"limit": 5}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/returns/company/orders", {"limit": 5}),
    ("GET", "https://statistics-api.wildberries.ru/api/v1/supplier/sales", {"dateFrom": date_from, "flag": 0}),
]

for method, url, params in candidates:
    r = requests.get(url, headers=headers, params=params, timeout=15)
    preview = r.text[:200].replace("\n", " ")
    print(f"{method} {url.split('wildberries.ru')[1]} → {r.status_code}: {preview}")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f"  Список: {len(data)} записей")
            if data:
                print(f"  Ключи первой записи: {list(data[0].keys())}")
        elif isinstance(data, dict):
            print(f"  Ключи: {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list) and v:
                    print(f"  {k}: {len(v)} записей, ключи: {list(v[0].keys())}")
    print()
