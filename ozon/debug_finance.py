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

# Период: 18–24 мая 2026
date_from = "2026-05-18T00:00:00.000Z"
date_to   = "2026-05-24T23:59:59.999Z"

# ── 1. Транзакции по типам ───────────────────────────────────────────────────
print("=== Транзакции 18–24 мая по типам ===")
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
        "page": 1,
        "page_size": 1000,
    },
    timeout=60,
)
print(f"HTTP: {r.status_code}")
if r.status_code == 200:
    ops = r.json().get("result", {}).get("operations", [])
    by_type = {}
    by_op_type = {}
    for op in ops:
        t = op.get("type", "?")
        ot = op.get("operation_type", "?")
        amt = float(op.get("amount", 0) or 0)
        by_type[t] = by_type.get(t, 0) + amt
        by_op_type[ot] = by_op_type.get(ot, 0) + amt

    print(f"Всего операций: {len(ops)}")
    print("\nПо type:")
    for k, v in sorted(by_type.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {k:20s}  {round(v, 2):>12} ₽")
    print("\nПо operation_type:")
    for k, v in sorted(by_op_type.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {k:50s}  {round(v, 2):>12} ₽")
    total = sum(by_type.values())
    print(f"\nИТОГО всех типов: {round(total, 2)} ₽")
    print(f"ИТОГО только orders: {round(by_type.get('orders', 0), 2)} ₽")

# ── 2. SKU → offer_id: разные варианты ──────────────────────────────────────
# Берём SKU из первой транзакции
sample_skus = []
if r.status_code == 200:
    for op in r.json().get("result", {}).get("operations", [])[:20]:
        for it in (op.get("items") or []):
            if it.get("sku"):
                sample_skus.append(int(it["sku"]))
    sample_skus = list(set(sample_skus))[:5]
    print(f"\nТестовые SKU: {sample_skus}")

def try_product(label, url, body):
    print(f"\n=== {label} ===")
    rr = requests.post(url, headers=headers, json=body, timeout=30)
    print(f"HTTP: {rr.status_code}")
    if rr.status_code == 200:
        print(json.dumps(rr.json(), indent=2, ensure_ascii=False)[:800])
    else:
        print(rr.text[:300])

if sample_skus:
    try_product(
        "v3/product/info/list с sku",
        "https://api-seller.ozon.ru/v3/product/info/list",
        {"sku": sample_skus},
    )
    try_product(
        "v2/product/info/list с sku",
        "https://api-seller.ozon.ru/v2/product/info/list",
        {"sku": sample_skus},
    )
    try_product(
        "v3/product/info/list с product_id",
        "https://api-seller.ozon.ru/v3/product/info/list",
        {"product_id": sample_skus},
    )

# Список всех товаров продавца
try_product(
    "v2/product/list (все товары)",
    "https://api-seller.ozon.ru/v2/product/list",
    {"filter": {}, "last_id": "", "limit": 3},
)
