import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - Выплаты"
WEEKS_TO_SHOW = 4


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


def get_weekly_periods(n):
    """Возвращает n последних полных недель (Пн–Вс) как пары строк дат."""
    now = datetime.now(timezone.utc)
    # Начало текущей недели (понедельник 00:00 UTC)
    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    periods = []
    for i in range(1, n + 1):
        start = week_start - timedelta(weeks=i)
        end = week_start - timedelta(weeks=i - 1) - timedelta(seconds=1)
        periods.append((start, end))
    return periods


def fetch_transactions(client_id, api_key, start, end):
    """Все транзакции за период с пагинацией."""
    all_ops = []
    page = 1
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/finance/transaction/list",
            headers=_headers(client_id, api_key),
            json={
                "filter": {
                    "date": {
                        "from": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "to": end.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
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
            print(f"  transactions error {r.status_code}: {r.text[:200]}")
            break
        result = r.json().get("result", {})
        ops = result.get("operations", [])
        all_ops.extend(ops)
        if len(ops) < 1000:
            break
        page += 1
    return all_ops


def build_sku_map(client_id, api_key, skus):
    """SKU (Ozon) → offer_id (артикул продавца) через product/info/list."""
    sku_map = {}
    skus = list(skus)
    for i in range(0, len(skus), 1000):
        batch = skus[i:i + 1000]
        r = requests.post(
            "https://api-seller.ozon.ru/v3/product/info/list",
            headers=_headers(client_id, api_key),
            json={"sku": batch},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"  product/info/list error {r.status_code}: {r.text[:200]}")
            continue
        items = r.json().get("result", {}).get("items", [])
        print(f"  product/info/list: получено {len(items)} товаров")
        if items:
            print(f"  Пример ответа: {items[0]}")
        for item in items:
            offer_id = item.get("offer_id", "")
            for field in ("sku", "fbo_sku", "fbs_sku"):
                sku = item.get(field)
                if sku and offer_id:
                    sku_map[sku] = offer_id
    return sku_map


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_msk = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    periods = get_weekly_periods(WEEKS_TO_SHOW)

    # Собираем транзакции по каждому периоду
    period_results = []
    all_skus = set()

    for start, end in periods:
        label = f"{start.strftime('%d.%m')}–{end.strftime('%d.%m.%Y')}"
        print(f"\nПериод {label}:")
        ops = fetch_transactions(client_id, api_key, start, end)
        print(f"  Транзакций: {len(ops)}")

        sku_amounts = defaultdict(float)
        total = 0.0

        for op in ops:
            amount = float(op.get("amount", 0) or 0)
            total += amount
            items = op.get("items") or []
            if items:
                per_item = amount / len(items)
                for it in items:
                    sku = it.get("sku")
                    if sku:
                        sku_amounts[int(sku)] += per_item
                        all_skus.add(int(sku))

        print(f"  Итого: {round(total, 2)} ₽, артикулов по SKU: {len(sku_amounts)}")
        period_results.append((label, total, dict(sku_amounts)))

    # Маппинг SKU → offer_id
    print(f"\nМаппинг {len(all_skus)} SKU...")
    sku_map = build_sku_map(client_id, api_key, all_skus)
    print(f"  Сопоставлено: {len(sku_map)} из {len(all_skus)}")

    # Перевод SKU → offer_id
    def to_offer_id_amounts(sku_amounts):
        result = defaultdict(float)
        for sku, amt in sku_amounts.items():
            offer_id = sku_map.get(sku, f"SKU:{sku}")
            result[offer_id] += amt
        return result

    # Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=5000, cols=4)

    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", "", ""],
    ]

    for label, total, sku_amounts in period_results:
        offer_amounts = to_offer_id_amounts(sku_amounts)
        sheet_rows.append(["", "", "", ""])
        sheet_rows.append([f"Период: {label}", "", f"ИТОГО: {fmt_money(total)} ₽", ""])
        sheet_rows.append(["Артикул", "Сумма к выплате", "", ""])
        for offer_id, amt in sorted(offer_amounts.items(), key=lambda x: x[1], reverse=True):
            sheet_rows.append([offer_id, fmt_money(amt) + " ₽", "", ""])

    ws.clear()
    ws.update(values=sheet_rows, range_name="A1")
    print(f"\nГотово! → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
