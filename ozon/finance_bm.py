import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

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


def fetch_realization(client_id, api_key, year, month):
    """Возвращает (header, rows) отчёта реализации за месяц."""
    r = requests.post(
        "https://api-seller.ozon.ru/v2/finance/realization",
        headers=_headers(client_id, api_key),
        json={"year": year, "month": month},
        timeout=60,
    )
    print(f"realization {year}-{month:02d}: HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  {r.text[:200]}")
        return None, []
    result = r.json().get("result", {})
    return result.get("header", {}), result.get("rows", [])


def calc_row_payout(row):
    """
    Расчёт чистой выплаты за одну строку реализации:
      total     = выручка после вычета комиссии Ozon (seller_price × (1 - commission_ratio))
      amount    = фактическая плата за логистику
      bonus     = компенсация логистики от Ozon
      net       = total - amount + bonus
    """
    dc = row.get("delivery_commission") or {}
    total = float(dc.get("total", 0) or 0)
    amount = float(dc.get("amount", 0) or 0)
    bonus = float(dc.get("bonus", 0) or 0)
    # Для возврата используем return_commission, если есть
    rc = row.get("return_commission") or {}
    return_total = float(rc.get("total", 0) or 0)
    return total - amount + bonus + return_total


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    # Пробуем текущий и предыдущий месяц
    months_to_try = [
        (now_utc.year, now_utc.month),
        ((now_utc.replace(day=1) - timedelta(days=1)).year,
         (now_utc.replace(day=1) - timedelta(days=1)).month),
    ]

    all_rows = []
    period_label = ""

    for year, month in months_to_try:
        header, rows = fetch_realization(client_id, api_key, year, month)
        if rows:
            print(f"  Строк: {len(rows)}")
            start = (header.get("start_date") or "")[:10]
            stop = (header.get("stop_date") or "")[:10]
            period_label = f"{start} – {stop}" if start and stop else f"{year}-{month:02d}"
            all_rows = rows
            break
        else:
            print(f"  Акт за {year}-{month:02d} ещё не сформирован, пробуем предыдущий")

    if not all_rows:
        print("Нет данных реализации")
        return

    # Группировка по артикулу
    article_payout = defaultdict(float)
    article_name = {}

    for row in all_rows:
        item = row.get("item") or {}
        offer_id = item.get("offer_id", "")
        if not offer_id:
            continue
        net = calc_row_payout(row)
        article_payout[offer_id] += net
        if offer_id not in article_name:
            article_name[offer_id] = item.get("name", "")[:60]

    total = sum(article_payout.values())
    print(f"Артикулов: {len(article_payout)}, итого: {round(total, 2)} ₽")

    # Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=5000, cols=5)

    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", "", ""],
        ["Период реализации:", period_label, "", ""],
        ["ИТОГО К ВЫПЛАТЕ:", fmt_money(total) + " ₽", "", ""],
        ["", "", "", ""],
        ["Артикул", "Название", "Сумма к выплате", ""],
    ]

    for offer_id, payout in sorted(article_payout.items(), key=lambda x: x[1], reverse=True):
        sheet_rows.append([offer_id, article_name.get(offer_id, ""), fmt_money(payout) + " ₽", ""])

    ws.clear()
    ws.update(values=sheet_rows, range_name="A1")
    print(f"Готово! Записано {len(article_payout)} артикулов → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
