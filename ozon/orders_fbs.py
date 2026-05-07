import os
import io
import csv
import json
import time
import requests
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1q7nDWrMge3XwlplH5LOBa0z6aryp7PcIkBIjclzorQ4"
SHEET_NAME = "Заказы BM"
DAYS_BACK = 30
GAP_COLS = 5

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
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def calc_spp(price, customer_price):
    try:
        p = float(str(price).replace(",", "."))
        cp = float(str(customer_price).replace(",", "."))
        return str(round((p - cp) / p, 10)).replace(".", ",") if cp and p else ""
    except (ValueError, TypeError):
        return ""


def fetch_fbs_orders(client_id, api_key):
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

        response = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=headers, json=payload, timeout=30,
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
                price = product.get("price", "")
                customer_price = fin.get("customer_price", "")
                all_rows.append([
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
                ])

        if len(postings) < limit:
            break
        offset += limit

    return all_rows


def fetch_fbo_via_report(client_id, api_key):
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

    # Шаг 1: создать отчёт
    r = requests.post(
        "https://api-seller.ozon.ru/v1/report/postings/create",
        headers=headers,
        json={
            "filter": {
                "processed_at_from": date_from,
                "processed_at_to": date_to,
                "delivery_schema": ["fbo"],
                "status": "",
            },
            "language": "DEFAULT",
        },
        timeout=30,
    )

    if r.status_code != 200:
        print(f"Ошибка создания отчёта FBO: {r.status_code} — {r.text}")
        return []

    code = r.json().get("result", {}).get("code", "")
    if not code:
        print("Не получен код отчёта FBO")
        return []

    print(f"Отчёт FBO создан, код: {code}. Ожидаю генерацию...")

    # Шаг 2: ждём готовности
    file_url = None
    for attempt in range(20):
        time.sleep(30)
        r2 = requests.post(
            "https://api-seller.ozon.ru/v1/report/info",
            headers=headers,
            json={"code": code},
            timeout=30,
        )

        if r2.status_code != 200:
            print(f"Ошибка статуса отчёта: {r2.status_code}")
            continue

        result = r2.json().get("result", {})
        status = result.get("status", "")
        print(f"Попытка {attempt + 1}: статус = {status}")

        if status == "success":
            file_url = result.get("file", "")
            break
        elif status == "failed":
            print(f"Отчёт не сгенерирован: {result.get('error', '')}")
            return []

    if not file_url:
        print("Таймаут: отчёт FBO не был готов за 10 минут")
        return []

    # Шаг 3: скачать CSV
    r3 = requests.get(file_url, timeout=120)
    if r3.status_code != 200:
        print(f"Ошибка скачивания отчёта: {r3.status_code}")
        return []

    # Шаг 4: парсить CSV
    content = r3.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")

    # Печатаем заголовки для отладки (первый запуск)
    fieldnames = reader.fieldnames or []
    print(f"Столбцы CSV: {fieldnames}")

    rows = []
    for row in reader:
        price = row.get("Ваша цена", "")
        customer_price = row.get("Оплачено", "")
        rows.append([
            row.get("Номер заказа", ""),
            row.get("Номер отправления", ""),
            row.get("Принят в обработку", ""),
            row.get("Дата отгрузки", ""),
            row.get("Статус", ""),
            row.get("Артикул", ""),
            fmt_num(price),
            row.get("Количество", ""),
            row.get("Кластер отгрузки", ""),
            row.get("Кластер доставки", ""),
            fmt_num(customer_price),
            calc_spp(price, customer_price),
        ])

    return rows


def write_report(fbs_rows, fbo_rows):
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=10000, cols=40)

    worksheet.clear()

    gap = [""] * GAP_COLS
    header_row = ["Заказы FBS"] + DATA_HEADERS + gap + ["Заказы FBO"] + DATA_HEADERS

    max_rows = max(len(fbs_rows), len(fbo_rows), 1)
    empty = [""] * len(DATA_HEADERS)
    fbs_padded = fbs_rows + [empty] * (max_rows - len(fbs_rows))
    fbo_padded = fbo_rows + [empty] * (max_rows - len(fbo_rows))

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    service = ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")]

    all_rows = [header_row]
    for i, (fbs, fbo) in enumerate(zip(fbs_padded, fbo_padded)):
        service_cell = service[i] if i < len(service) else ""
        all_rows.append([service_cell] + fbs + gap + [""] + fbo)

    worksheet.update("A1", all_rows)
    print(f"FBS: {len(fbs_rows)} строк, FBO: {len(fbo_rows)} строк → лист '{SHEET_NAME}'")


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю заказы Ozon BM за последние {DAYS_BACK} дней...")

    fbs_rows = fetch_fbs_orders(client_id, api_key)
    print(f"FBS: {len(fbs_rows)} строк")

    fbo_rows = fetch_fbo_via_report(client_id, api_key)
    print(f"FBO: {len(fbo_rows)} строк")

    write_report(fbs_rows, fbo_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
