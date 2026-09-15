import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}

now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период: {date_from} – {date_to}")
print()

# 1. Список рекламных кампаний
r = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/promotion/count",
    headers=headers,
    timeout=30,
)
print(f"GET /adv/v1/promotion/count → {r.status_code}")
if r.status_code == 200:
    print(f"  {r.text[:300]}")

print()

# 2. Список кампаний с деталями
r2 = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/promotion/adverts",
    headers=headers,
    params={"status": 9, "limit": 5},  # 9 = активные
    timeout=30,
)
print(f"GET /adv/v1/promotion/adverts (активные) → {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    if data:
        print(f"  Кампаний: {len(data)}")
        print(f"  Первая кампания:")
        print(json.dumps(data[0], ensure_ascii=False, indent=2)[:500])

print()

# 3. Все статусы
r3 = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/promotion/adverts",
    headers=headers,
    params={"limit": 50},
    timeout=30,
)
print(f"GET /adv/v1/promotion/adverts (все) → {r3.status_code}")
if r3.status_code == 200:
    data = r3.json() or []
    print(f"  Кампаний: {len(data)}")
    if data:
        ids = [c.get("advertId") for c in data[:10] if c.get("advertId")]
        print(f"  ID первых 10: {ids}")

print()

# 4. Расходы по кампаниям
r4 = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/upd",
    headers=headers,
    params={"from": date_from, "to": date_to},
    timeout=30,
)
print(f"GET /adv/v1/upd (расходы) → {r4.status_code}")
if r4.status_code == 200:
    data = r4.json()
    print(f"  {json.dumps(data, ensure_ascii=False)[:500]}")
else:
    print(f"  {r4.text[:300]}")
