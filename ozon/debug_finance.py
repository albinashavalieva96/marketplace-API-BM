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

# ── 1. Гипотеза: выплата = транзакции где order_date в периоде выплаты ───────
# Берём транзакции за последние 30 дней (май 18 – июн 17)
# и фильтруем по order_date = 18–24 мая

print("=== Транзакции май 18 – июн 17, фильтр по order_date 18–24 мая ===")
all_ops = []
page = 1
while True:
    r = requests.post(
        "https://api-seller.ozon.ru/v3/finance/transaction/list",
        headers=headers,
        json={
            "filter": {
                "date": {"from": "2026-05-18T00:00:00.000Z",
                         "to":   "2026-06-17T23:59:59.999Z"},
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

# Фильтруем по order_date в 18–24 мая
total_all = 0.0
total_order_filtered = 0.0
skus_filtered = set()

for op in all_ops:
    total_all += float(op.get("amount", 0) or 0)
    order_date = (op.get("posting") or {}).get("order_date", "")
    if "2026-05-18" <= order_date[:10] <= "2026-05-24":
        total_order_filtered += float(op.get("amount", 0) or 0)
        for it in (op.get("items") or []):
            if it.get("sku"):
                skus_filtered.add(int(it["sku"]))

print(f"\nВсего операций: {len(all_ops)}")
print(f"ИТОГО весь период (май18–июн17): {round(total_all, 2)} ₽")
print(f"ИТОГО order_date 18–24 мая:      {round(total_order_filtered, 2)} ₽")
print(f"  (ожидаем ≈ 193 538 ₽)")
print(f"Уникальных SKU в фильтре: {len(skus_filtered)}")

# ── 2. Маппинг через реализацию ──────────────────────────────────────────────
print("\n=== Маппинг SKU→offer_id через реализацию (май) ===")
r2 = requests.post(
    "https://api-seller.ozon.ru/v2/finance/realization",
    headers=headers,
    json={"year": 2026, "month": 5},
    timeout=60,
)
print(f"HTTP: {r2.status_code}")
if r2.status_code == 200:
    rows = r2.json().get("result", {}).get("rows", [])
    sku_to_offer = {}
    for row in rows:
        item = row.get("item") or {}
        sku = item.get("sku")
        offer_id = item.get("offer_id", "")
        if sku and offer_id:
            sku_to_offer[sku] = offer_id
    print(f"SKU в реализации: {len(sku_to_offer)}")

    # Проверяем покрытие наших SKU из транзакций
    found = sum(1 for s in skus_filtered if s in sku_to_offer)
    print(f"Покрытие SKU из транзакций: {found}/{len(skus_filtered)}")
    print(f"Примеры маппинга: {dict(list(sku_to_offer.items())[:5])}")
else:
    print(r2.text[:200])
