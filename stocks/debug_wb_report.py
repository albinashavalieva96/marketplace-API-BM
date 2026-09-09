import os
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_BAR_API_KEY"]
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период: {date_from} – {date_to}")

r = requests.get(
    "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod",
    headers={"Authorization": f"Bearer {api_key}"},
    params={"dateFrom": date_from, "dateTo": date_to, "rrdid": 0},
    timeout=120,
)
print(f"Статус: {r.status_code}")

if r.status_code != 200:
    print(f"Ошибка: {r.text[:500]}")
else:
    rows = r.json()
    print(f"Строк: {len(rows)}")
    if rows:
        print("Пример первой строки (все поля):")
        for k, v in rows[0].items():
            print(f"  {k}: {v}")
        print()
        print("Уникальные sa_name (первые 10):")
        articles = list({r.get("sa_name", "") for r in rows if r.get("sa_name")})[:10]
        for a in articles:
            print(f"  {a}")
    else:
        print("Пустой ответ")
