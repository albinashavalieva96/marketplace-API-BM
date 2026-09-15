import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_token = os.environ["YM_VIZ_API_TOKEN"]
headers = {"Api-Key": api_token}

now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=90)).strftime("%d-%m-%Y")
date_to = now.strftime("%d-%m-%Y")

campaign_id = 22110675  # FBY Виз

print(f"Период: {date_from} – {date_to}")
print()

# Подробно смотрим /stats/orders
r = requests.post(
    f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders",
    headers=headers,
    json={"dateFrom": date_from, "dateTo": date_to},
    timeout=30,
)
print(f"POST /stats/orders → {r.status_code}")
data = r.json().get("result", {})
orders = data.get("orders", [])
print(f"Заказов: {len(orders)}")
if orders:
    print("Поля первого заказа:")
    print(json.dumps(orders[0], ensure_ascii=False, indent=2)[:2000])

print()

# Пробуем другие эндпоинты для рекламных расходов
other_endpoints = [
    ("GET", f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/skus", None),
    ("POST", f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/show-sales", {"dateFrom": date_from, "dateTo": date_to}),
    ("GET", f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/promos", None),
    ("POST", f"https://api.partner.market.yandex.ru/v2/businesses/0/promos", {"dateFrom": date_from}),
]

for method, url, body in other_endpoints:
    path = url.split(".ru")[-1]
    if method == "GET":
        r = requests.get(url, headers=headers, timeout=15)
    else:
        r = requests.post(url, headers=headers, json=body, timeout=15)
    print(f"{method} {path} → {r.status_code}")
    if r.status_code == 200:
        print(f"  {r.text[:300]}")
