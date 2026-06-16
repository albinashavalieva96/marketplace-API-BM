import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")

# Шаг 1: берём 10 заказов из маркетплейс API
print("=== Шаг 1: GET /api/v3/orders (первые 10) ===")
r = requests.get(
    "https://marketplace-api.wildberries.ru/api/v3/orders",
    headers=headers,
    params={"limit": 10, "next": 0},
    timeout=30,
)
mp_orders = r.json().get("orders", [])
print(f"Получено: {len(mp_orders)} заказов")
for o in mp_orders[:3]:
    print(f"  id={o['id']}  orderUid={o['orderUid']}  article={o['article']}  createdAt={o['createdAt'][:10]}")

# Шаг 2: получаем статусы по ID
print("\n=== Шаг 2: POST /api/v3/orders/status ===")
ids = [o["id"] for o in mp_orders]
r2 = requests.post(
    "https://marketplace-api.wildberries.ru/api/v3/orders/status",
    headers=headers,
    json={"orders": ids},
    timeout=30,
)
print(f"HTTP: {r2.status_code}")
if r2.status_code == 200:
    statuses = r2.json().get("orders", [])
    print(f"Статусов получено: {len(statuses)}")
    for s in statuses[:5]:
        print(f"  id={s['id']}  supplierStatus={s.get('supplierStatus')}  wbStatus={s.get('wbStatus')}")
else:
    print(r2.text[:300])

# Шаг 3: берём 10 заказов из статистического API
print("\n=== Шаг 3: GET /v1/supplier/orders (первые 10) ===")
r3 = requests.get(
    "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
    headers=headers,
    params={"dateFrom": date_from, "flag": 0},
    timeout=60,
)
stat_orders = r3.json()[:10] if r3.status_code == 200 else []
print(f"Получено: {len(stat_orders)} заказов")
for o in stat_orders[:3]:
    print(f"  srid={o.get('srid')}  article={o.get('supplierArticle')}  date={str(o.get('date',''))[:10]}")

# Шаг 4: проверяем совпадение srid и orderUid
print("\n=== Шаг 4: Проверка srid ↔ orderUid ===")
mp_uid_set = {o["orderUid"] for o in mp_orders}
mp_uid_stripped = {o["orderUid"][1:] for o in mp_orders if o["orderUid"]}
stat_srid_set = {o.get("srid", "") for o in stat_orders if o.get("srid")}

match_exact = stat_srid_set & mp_uid_set
match_stripped = stat_srid_set & mp_uid_stripped
print(f"Точных совпадений srid == orderUid: {len(match_exact)}")
print(f"Совпадений srid == orderUid[1:]: {len(match_stripped)}")
if match_exact:
    print(f"  Пример: {list(match_exact)[0]}")
if match_stripped:
    print(f"  Пример srid: {list(match_stripped)[0]}")
