import os
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

api_key = os.environ["WB_BAR_API_KEY"]
now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период: {date_from} – {date_to}")

r = requests.get(
    "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
    headers={"Authorization": f"Bearer {api_key}"},
    params={"dateFrom": date_from, "dateTo": date_to, "rrdid": 0},
    timeout=120,
)
print(f"Статус: {r.status_code}")
rows = r.json() if r.status_code == 200 else []
print(f"Строк всего: {len(rows)}")

# Группировка по типу операции
by_oper = defaultdict(int)
for row in rows:
    by_oper[row.get("supplier_oper_name", "")] += 1
print("\nТипы операций:")
for oper, cnt in sorted(by_oper.items(), key=lambda x: -x[1]):
    print(f"  {oper}: {cnt}")

# Примеры строк с количеством > 0
print("\nПримеры строк с quantity > 0:")
examples = [r for r in rows if int(r.get("quantity", 0) or 0) > 0][:5]
for ex in examples:
    print(f"  sa_name={ex.get('sa_name')}  qty={ex.get('quantity')}  ppvz_for_pay={ex.get('ppvz_for_pay')}  oper={ex.get('supplier_oper_name')}")

# Строки с quantity = 0
zero_qty = sum(1 for r in rows if int(r.get("quantity", 0) or 0) == 0)
print(f"\nСтрок с quantity=0: {zero_qty} из {len(rows)}")
