import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}
now = datetime.now(timezone.utc)
date_from = int((now - timedelta(days=7)).timestamp() * 1000)  # milliseconds
date_from_iso = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")

candidates = [
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/orders",
     {"limit": 5, "next": 0}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/orders",
     {"limit": 5, "next": 0, "dateFrom": date_from}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/orders/new",
     {"limit": 5}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/fbs/orders",
     {"limit": 5, "next": 0}),
    ("GET", "https://marketplace-api.wildberries.ru/api/v3/orders/status",
     {"limit": 5}),
]

for method, url, params in candidates:
    r = requests.get(url, headers=headers, params=params, timeout=15)
    path = url.split("wildberries.ru")[1]
    preview = r.text[:300].replace("\n", " ")
    print(f"GET {path} → {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f"  Список: {len(data)} записей")
            if data:
                print(f"  Ключи: {list(data[0].keys())}")
                print(f"  Первая запись: {json.dumps(data[0], ensure_ascii=False)}")
        elif isinstance(data, dict):
            print(f"  Ключи верхнего уровня: {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list):
                    print(f"  {k}: {len(v)} записей")
                    if v:
                        print(f"  Ключи первой: {list(v[0].keys())}")
                        print(f"  Первая запись: {json.dumps(v[0], ensure_ascii=False)}")
                else:
                    print(f"  {k}: {v}")
        print("=== РАБОТАЕТ ===")
    else:
        print(f"  {preview}")
    print()
