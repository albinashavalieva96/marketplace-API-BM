import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - Выплаты"


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


def fetch_pending_total(client_id, api_key):
    """Возвращает общую сумму, ожидающую выплаты."""
    r = requests.post(
        "https://api-seller.ozon.ru/v1/finance/treasury/totals",
        headers=_headers(client_id, api_key),
        json={},
        timeout=30,
    )
    print(f"treasury/totals HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  Ошибка: {r.text[:300]}")
        return 0
    result = r.json().get("result", {})
    print(f"  Ответ: {result}")
    return (
        result.get("awaiting_payment_amount")
        or result.get("payout")
        or result.get("total")
        or 0
    )


def fetch_realization(client_id, api_key, year_month):
    """Возвращает строки отчёта реализации за месяц YYYY-MM."""
    r = requests.post(
        "https://api-seller.ozon.ru/v3/finance/realization",
        headers=_headers(client_id, api_key),
        json={"date": year_month},
        timeout=30,
    )
    print(f"realization {year_month}: HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  Ошибка: {r.text[:300]}")
        return []
    result = r.json().get("result", {})
    rows = result.get("rows", [])
    print(f"  Строк: {len(rows)}")
    if rows:
        print(f"  Пример строки: {rows[0]}")
    return rows


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Общая сумма к выплате
    total_pending = fetch_pending_total(client_id, api_key)
    print(f"Итого к выплате: {total_pending}")

    # 2. По артикулам — текущий и предыдущий месяц
    current_month = now_utc.strftime("%Y-%m")
    prev_month = (now_utc.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    article_totals = {}
    for month in [current_month, prev_month]:
        rows = fetch_realization(client_id, api_key, month)
        for row in rows:
            offer_id = row.get("offer_id", "")
            payout = float(row.get("payout", row.get("delivery_amount", 0)) or 0)
            if offer_id and payout:
                article_totals[offer_id] = article_totals.get(offer_id, 0) + payout

    # 3. Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=5)

    period_label = f"{prev_month} / {current_month}"
    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", ""],
        ["ИТОГО К ВЫПЛАТЕ:", fmt_money(total_pending) + " ₽", ""],
        ["", "", ""],
        ["Артикул", "Сумма к выплате", "Период"],
    ]
    for offer_id, total in sorted(article_totals.items(), key=lambda x: x[1], reverse=True):
        sheet_rows.append([offer_id, fmt_money(total) + " ₽", period_label])

    ws.clear()
    ws.update("A1", sheet_rows)
    print(f"Готово! Записано {len(article_totals)} артикулов → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
