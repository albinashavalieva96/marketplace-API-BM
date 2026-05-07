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

print("=== Проверка FBO ===")

# Вариант 1: стандартный запрос без financial_data
payload1 = {
    "dir": "DESC",
    "filter": {
        "since": (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": now.strftime("%Y-%m-%dT23:59:59.999Z"),
        "status": "",
    },
    "limit": 10,
    "offset": 0,
    "with": {"analytics_data": False, "financial_data": False},
}
r1 = requests.post("https://api-seller.ozon.ru/v3/posting/fbo/list", headers=headers, json=payload1, timeout=30)
d1 = r1.json()
p1 = d1.get("result", {}).get("postings", [])
print(f"v3 без financial_data (90 дней): {r1.status_code}, найдено: {len(p1)}")
if r1.status_code != 200:
    print(f"Ошибка: {d1}")

# Вариант 2: v2 endpoint
payload2 = {
    "dir": "DESC",
    "filter": {
        "since": (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": now.strftime("%Y-%m-%dT23:59:59.999Z"),
        "status": "",
    },
    "limit": 10,
    "offset": 0,
    "translit": False,
    "with": {"analytics_data": False, "financial_data": False},
}
r2 = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json=payload2, timeout=30)
d2 = r2.json()
p2 = d2.get("result", [])
print(f"v2 (90 дней): {r2.status_code}, найдено: {len(p2)}")
if r2.status_code != 200:
    print(f"Ошибка: {d2}")
