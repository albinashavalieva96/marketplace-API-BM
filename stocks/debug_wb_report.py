import os
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_BAR_API_KEY"]
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период: {date_from} – {date_to}")

endpoints = [
    ("statistics-api.wildberries.ru", "/api/v1/supplier/reportDetailByPeriod"),
    ("statistics-api.wildberries.ru", "/api/v5/supplier/reportDetailByPeriod"),
    ("seller-analytics-api.wildberries.ru", "/api/v1/supplier/reportDetailByPeriod"),
    ("seller-analytics-api.wildberries.ru", "/api/v2/supplier/reportDetailByPeriod"),
]

for host, path in endpoints:
    url = f"https://{host}{path}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "dateTo": date_to, "rrdid": 0},
        timeout=30,
    )
    print(f"{url} → {r.status_code}")
    if r.status_code == 200:
        rows = r.json() if isinstance(r.json(), list) else []
        print(f"  Строк: {len(rows)}")
        if rows:
            print(f"  Поля: {list(rows[0].keys())}")
            print(f"  Пример sa_name: {rows[0].get('sa_name', '???')}")
        break
    else:
        print(f"  {r.text[:200]}")
