import os
import sys
import json
import requests
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1q7nDWrMge3XwlplH5LOBa0z6aryp7PcIkBIjclzorQ4"
SHEET_NAME = "FBS Заказы BM"
DAYS_BACK = 30

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DATA_HEADERS = [
    "Номер заказа",
    "Номер отправления",
    "Принят в обработку",
    "Дата отгрузки",
    "Статус",
    "Артикул",
    "Цена продажи",
    "Количество",
    "Кластер отгрузки",
    "Кластер доставки",
    "Оплатили",
    "СПП",
]

STATUS_MAP = {
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
    "delivering": "Доставляется",
    "delivered": "Доставлено",
    "cancelled": "Отменено",
    "not_accepted": "Не принято на сортировке",
}


def fmt_dt(value):
    if not value:
        return ""
    return value[:19].replace("T", " ")


def _calc_spp(price, customer_price):
    try:
        p = float(str(price).replace(",", "."))
        cp = float(str(customer_price).replace(",", "."))
        return round(p - cp, 2) if cp else ""
    except (ValueError, TypeError):
        return ""


def fetch_orders(client_id, api_key):
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

    all_rows = []
    offset = 0
    limit = 100

    while True:
        payload = {
            "dir": "DESC",
            "filter": {
                "since": date_from,
                "to": date_to,
                "status": "",
            },
            "limit": limit,
            "offset": offset,
            "with": {
                "analytics_data": False,
                "financial_data": True,
            },
        }

        response = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=headers,
            json=payload,
            timeout=30,
        )

        if response.status_code != 200:
            print(f"Ошибка FBS: {response.status_code} — {response.text}")
            break

        postings = response.json().get("result", {}).get("postings", [])

        for posting in postings:
            financial = posting.get("financial_data") or {}
            fin_products = financial.get("products") or []

            status = STATUS_MAP.get(posting.get("status", ""), posting.get("status", ""))

            for i, product in enumerate(posting.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                row = [
                    posting.get("order_number", ""),
                    posting.get("posting_number", ""),
                    fmt_dt(posting.get("in_process_at", "")),
                    fmt_dt(posting.get("shipment_date", "")),
                    status,
                    product.get("offer_id", ""),
                    product.get("price", ""),
                    product.get("quantity", 0),
                    financial.get("cluster_from", ""),
                    financial.get("cluster_to", ""),
                    fin.get("customer_price", ""),
                    _calc_spp(product.get("price"), fin.get("customer_price")),
                ]
                all_rows.append(row)

        if len(postings) < limit:
            break
        offset += limit

    return all_rows


def write_report(rows):
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=10000, cols=20)

    worksheet.clear()

    # Строка 1: заголовок служебного столбца A + заголовки данных
    header_row = ["Заказы с наших складов с кластерами"] + DATA_HEADERS

    # Строки данных: пустой столбец A + данные
    all_rows = [header_row] + [[""] + row for row in rows]
    worksheet.update("A1", all_rows)

    # Служебные ячейки: A2 = "Обновлен:", A3 = дата, A4 = время
    now = datetime.now()
    worksheet.update("A2", [["Обновлен:"], [now.strftime("%Y-%m-%d")], [now.strftime("%H:%M")]])

    print(f"Записано {len(rows)} строк в лист '{SHEET_NAME}'")


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю FBS заказы Ozon BM за последние {DAYS_BACK} дней...")

    rows = fetch_orders(client_id, api_key)
    print(f"Получено строк: {len(rows)}")

    write_report(rows)
    print("Готово!")


if __name__ == "__main__":
    main()
