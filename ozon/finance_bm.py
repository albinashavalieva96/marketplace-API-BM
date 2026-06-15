import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - Баланс по артикулам"
BALANCE_DAYS = 28  # горизонт неоплаченных периодов ≈ 4 недели


def _headers(client_id, api_key):
    return {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }


def fmt_money(value):
    try:
        return str(round(float(value), 2)).replace(".", ",")
    except (ValueError, TypeError):
        return "0,00"


def fetch_sku_map(client_id, api_key):
    """Маппинг SKU → offer_id из отчёта реализации прошлого месяца."""
    now = datetime.now(timezone.utc)
    prev = now.replace(day=1) - timedelta(days=1)
    r = requests.post(
        "https://api-seller.ozon.ru/v2/finance/realization",
        headers=_headers(client_id, api_key),
        json={"year": prev.year, "month": prev.month},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"  realization error: {r.status_code}")
        return {}
    rows = r.json().get("result", {}).get("rows", [])
    sku_map = {}
    for row in rows:
        item = row.get("item") or {}
        sku = item.get("sku")
        offer_id = item.get("offer_id", "")
        if sku and offer_id:
            sku_map[sku] = offer_id
    print(f"  Маппинг: {len(sku_map)} SKU из реализации {prev.year}-{prev.month:02d}")
    return sku_map


def fetch_all_transactions(client_id, api_key, date_from, date_to):
    """Все транзакции за период (с пагинацией)."""
    all_ops = []
    page = 1
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/finance/transaction/list",
            headers=_headers(client_id, api_key),
            json={
                "filter": {
                    "date": {
                        "from": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                        "to":   date_to.strftime("%Y-%m-%dT23:59:59.999Z"),
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": 1000,
            },
            timeout=60,
        )
        if r.status_code != 200:
            print(f"  transactions error: {r.status_code}")
            break
        ops = r.json().get("result", {}).get("operations", [])
        all_ops.extend(ops)
        if len(ops) < 1000:
            break
        page += 1
    return all_ops


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Маппинг SKU → offer_id
    sku_map = fetch_sku_map(client_id, api_key)

    # 2. Транзакции за последние BALANCE_DAYS дней
    date_from = now_utc - timedelta(days=BALANCE_DAYS)
    date_to = now_utc
    print(f"Транзакции: {date_from.strftime('%d.%m')} – {date_to.strftime('%d.%m.%Y')}")

    ops = fetch_all_transactions(client_id, api_key, date_from, date_to)
    print(f"Всего транзакций: {len(ops)}")

    # 3. Группировка по артикулу
    offer_amounts = defaultdict(float)
    unmatched_total = 0.0
    total_balance = 0.0

    for op in ops:
        amount = float(op.get("amount", 0) or 0)
        total_balance += amount
        items = op.get("items") or []
        if items:
            per_item = amount / len(items)
            for it in items:
                sku = it.get("sku")
                if sku:
                    offer_id = sku_map.get(int(sku), "")
                    if offer_id:
                        offer_amounts[offer_id] += per_item
                    else:
                        unmatched_total += per_item
        else:
            unmatched_total += amount

    print(f"Итого транзакций: {round(total_balance, 2)} ₽")
    print(f"Распределено по артикулам: {round(sum(offer_amounts.values()), 2)} ₽")
    print(f"Не сопоставлено с артикулом: {round(unmatched_total, 2)} ₽")

    # 4. Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=5000, cols=4)

    period = f"{date_from.strftime('%d.%m')} – {now_msk.strftime('%d.%m.%Y')}"
    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", "", ""],
        [f"Период: {period}", "", "", ""],
        [f"БАЛАНС (начислено за период):", fmt_money(total_balance) + " ₽", "", ""],
        ["", "", "", ""],
        ["Артикул", "Сумма", "Доля, %", ""],
    ]

    sorted_items = sorted(offer_amounts.items(), key=lambda x: x[1], reverse=True)
    distributable = sum(offer_amounts.values())

    for offer_id, amt in sorted_items:
        share = round(amt / distributable * 100, 1) if distributable else 0
        sheet_rows.append([offer_id, fmt_money(amt) + " ₽", str(share).replace(".", ",") + "%", ""])

    if unmatched_total:
        sheet_rows.append(["(прочие/сервисы)", fmt_money(unmatched_total) + " ₽", "", ""])

    ws.clear()
    ws.update(values=sheet_rows, range_name="A1")
    print(f"Готово! Записано {len(sorted_items)} артикулов → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
