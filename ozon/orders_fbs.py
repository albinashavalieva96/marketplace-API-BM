import os
import json
import requests
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1q7nDWrMge3XwlplH5LOBa0z6aryp7PcIkBIjclzorQ4"
SHEET_NAME = "Заказы BM"
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


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def calc_spp(price, customer_price):
    try:
        p = float(str(price).replace(",", "."))
        cp = float(str(customer_price).replace(",", "."))
        return str(round((p - cp) / p, 10)).replace(".", ",") if cp and p else ""
    except (ValueError, TypeError):
        return ""


def fetch_orders(client_id, api_key, schema):
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
            "filter": {"since": date_from, "to": date_to, "status": ""},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": True},
        }

        # FBO использует v2 (v3 возвращает пустой список)
        if schema == "fbo":
            url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
        else:
            url = f"https://api-seller.ozon.ru/v3/posting/{schema}/list"

        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code != 200:
            print(f"Ошибка {schema.upper()}: {response.status_code} — {response.text}")
            break

        data = response.json()
        # v2 FBO: result — массив; v3 FBS: result.postings — массив
        if schema == "fbo":
            postings = data.get("result", [])
        else:
            postings = data.get("result", {}).get("postings", [])

        for posting in postings:
            financial = posting.get("financial_data") or {}
            fin_products = financial.get("products") or []
            status = STATUS_MAP.get(posting.get("status", ""), posting.get("status", ""))

            for i, product in enumerate(posting.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                price = product.get("price", "")
                customer_price = fin.get("customer_price", "")
                row = [
                    posting.get("order_number", ""),
                    posting.get("posting_number", ""),
                    fmt_dt(posting.get("in_process_at", "")),
                    fmt_dt(posting.get("shipment_date", "")),
                    status,
                    product.get("offer_id", ""),
                    fmt_num(price),
                    product.get("quantity", 0),
                    financial.get("cluster_from", ""),
                    financial.get("cluster_to", ""),
                    fmt_num(customer_price),
                    calc_spp(price, customer_price),
                ]
                all_rows.append(row)

        if len(postings) < limit:
            break
        offset += limit

    return all_rows


def write_report(fbs_rows, fbo_rows):
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=10000, cols=30)

    worksheet.clear()

    # Строка 1: заголовки FBS + заголовки FBO
    header_row = ["Заказы FBS"] + DATA_HEADERS + ["Заказы FBO"] + DATA_HEADERS

    # Выравниваем длины списков
    max_rows = max(len(fbs_rows), len(fbo_rows))
    empty = [""] * len(DATA_HEADERS)
    fbs_padded = fbs_rows + [empty] * (max_rows - len(fbs_rows))
    fbo_padded = fbo_rows + [empty] * (max_rows - len(fbo_rows))

    # Объединяем строки: [пусто(FBS service)] + FBS данные + [пусто(FBO service)] + FBO данные
    all_rows = [header_row]
    for fbs, fbo in zip(fbs_padded, fbo_padded):
        all_rows.append([""] + fbs + [""] + fbo)

    worksheet.update("A1", all_rows)

    # Служебные ячейки (московское время UTC+3)
    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    worksheet.update("A2", [["Обновлен:"], [now.strftime("%Y-%m-%d")], [now.strftime("%H:%M")]])

    print(f"FBS: {len(fbs_rows)} строк, FBO: {len(fbo_rows)} строк → лист '{SHEET_NAME}'")


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю заказы Ozon BM за последние {DAYS_BACK} дней...")

    fbs_rows = fetch_orders(client_id, api_key, "fbs")
    fbo_rows = fetch_orders(client_id, api_key, "fbo")

    write_report(fbs_rows, fbo_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
