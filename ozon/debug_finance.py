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

date_from = "2026-05-18T00:00:00.000Z"
date_to   = "2026-05-24T23:59:59.999Z"

# ── 1. Все транзакции за период (все страницы) ───────────────────────────────
print("=== Все транзакции 18–24 мая ===")
all_ops = []
page = 1
while True:
    r = requests.post(
        "https://api-seller.ozon.ru/v3/finance/transaction/list",
        headers=headers,
        json={
            "filter": {
                "date": {"from": date_from, "to": date_to},
                "operation_type": [],
                "posting_number": "",
                "transaction_type": "all",
            },
            "page": page,
            "page_size": 1000,
        },
        timeout=60,
    )
    ops = r.json().get("result", {}).get("operations", [])
    all_ops.extend(ops)
    print(f"  Страница {page}: {len(ops)} операций")
    if len(ops) < 1000:
        break
    page += 1

by_type = {}
by_op_type = {}
all_skus = set()
for op in all_ops:
    t = op.get("type", "?")
    ot = op.get("operation_type", "?")
    amt = float(op.get("amount", 0) or 0)
    by_type[t] = by_type.get(t, 0) + amt
    by_op_type[ot] = by_op_type.get(ot, 0) + amt
    for it in (op.get("items") or []):
        if it.get("sku"):
            all_skus.add(int(it["sku"]))

print(f"\nВсего операций: {len(all_ops)}, уникальных SKU: {len(all_skus)}")
print("\nПо type:")
for k, v in sorted(by_type.items(), key=lambda x: abs(x[1]), reverse=True):
    print(f"  {k:20s}  {round(v, 2):>12} ₽")
total_all = sum(by_type.values())
total_orders = by_type.get("orders", 0)
print(f"\nИТОГО all:    {round(total_all, 2)} ₽")
print(f"ИТОГО orders: {round(total_orders, 2)} ₽")

# ── 2. Полный ответ product/info/list (первые 5 SKU) ────────────────────────
sample_skus = list(all_skus)[:5]
print(f"\n=== product/info/list, sku={sample_skus} ===")
r2 = requests.post(
    "https://api-seller.ozon.ru/v3/product/info/list",
    headers=headers,
    json={"sku": sample_skus},
    timeout=30,
)
print(f"HTTP: {r2.status_code}")
if r2.status_code == 200:
    items = r2.json().get("result", {}).get("items", [])
    print(f"Товаров в ответе: {len(items)}")
    if items:
        # Печатаем первый товар полностью
        print("--- Первый товар (все поля) ---")
        first = items[0]
        for k, v in first.items():
            if k != "images":  # пропускаем картинки
                print(f"  {k}: {v}")
