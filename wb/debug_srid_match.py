import os
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")

print("=== Продажи (sales, saleID=S*) ===")
r = requests.get(
    "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
    headers=headers,
    params={"dateFrom": date_from, "flag": 0},
    timeout=120,
)
sales = r.json() if r.status_code == 200 else []
sales_s = [s for s in sales if str(s.get("saleID", "")).startswith("S")]
print(f"Всего продаж: {len(sales)}, из них S*: {len(sales_s)}")
print("Примеры srid из продаж:")
for s in sales_s[:5]:
    print(f"  saleID={s.get('saleID')}  srid={s.get('srid')}  article={s.get('supplierArticle')}")

print()
print("=== Заказы (orders) ===")
r2 = requests.get(
    "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
    headers=headers,
    params={"dateFrom": date_from, "flag": 0},
    timeout=120,
)
orders = r2.json() if r2.status_code == 200 else []
print(f"Всего заказов: {len(orders)}")
print("Примеры srid из заказов (FBO, не отменены):")
fbo_orders = [o for o in orders if o.get("warehouseType") == "Склад WB" and not o.get("isCancel")]
for o in fbo_orders[:5]:
    print(f"  srid={o.get('srid')}  article={o.get('supplierArticle')}  date={str(o.get('date',''))[:10]}")

print()
print("=== Проверка совпадений ===")
sales_srids = {s.get("srid") for s in sales_s if s.get("srid")}
order_srids = {o.get("srid") for o in fbo_orders if o.get("srid")}
matched = order_srids & sales_srids
print(f"FBO заказов: {len(order_srids)}")
print(f"Совпадений srid заказ == srid продажа: {len(matched)}")
if matched:
    print(f"  Пример: {list(matched)[0]}")

not_matched = order_srids - sales_srids
print(f"Не совпало (В работе): {len(not_matched)}")
if not_matched:
    example = list(not_matched)[0]
    example_order = next(o for o in fbo_orders if o.get("srid") == example)
    print(f"  Пример srid: {example}")
    print(f"  Дата заказа: {str(example_order.get('date',''))[:10]}")
    print(f"  isCancel: {example_order.get('isCancel')}")
    print(f"  lastChangeDate: {str(example_order.get('lastChangeDate',''))[:10]}")
