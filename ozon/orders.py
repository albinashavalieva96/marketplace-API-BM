import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_to_sheet

SPREADSHEET_ID = "1q7nDWrMge3XwlplH5LOBa0z6aryp7PcIkBIjclzorQ4"
SHEET_NAME = "Заказы Ozon BM"
DAYS_BACK = 30

HEADERS = [
    "Тип (FBO/FBS)",
    "Номер отправления",
    "Номер заказа",
    "Статус",
    "Дата создания",
    "Артикул продавца",
    "SKU Ozon",
    "Название товара",
    "Количество",
    "Цена",
    "Регион доставки",
    "Способ доставки",
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

    for schema in ["fbo", "fbs"]:
        offset = 0
        limit = 1000

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
                    "analytics_data": True,
                    "financial_data": False,
                },
            }

            url = f"https://api-seller.ozon.ru/v3/posting/{schema}/list"
            response = requests.post(url, headers=headers, json=payload, timeout=30)

            if response.status_code != 200:
                print(f"Ошибка {schema}: {response.status_code} — {response.text}")
                break

            postings = response.json().get("result", {}).get("postings", [])

            for posting in postings:
                analytics = posting.get("analytics_data") or {}
                status_raw = posting.get("status", "")
                status = STATUS_MAP.get(status_raw, status_raw)
                created_at = posting.get("created_at", "")[:19].replace("T", " ")

                for product in posting.get("products", []):
                    row = [
                        schema.upper(),
                        posting.get("posting_number", ""),
                        posting.get("order_number", ""),
                        status,
                        created_at,
                        product.get("offer_id", ""),
                        product.get("sku", ""),
                        product.get("name", ""),
                        product.get("quantity", 0),
                        product.get("price", ""),
                        analytics.get("region", ""),
                        analytics.get("delivery_type", ""),
                    ]
                    all_rows.append(row)

            if len(postings) < limit:
                break
            offset += limit

    return all_rows


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю заказы Ozon BM за последние {DAYS_BACK} дней...")

    rows = fetch_orders(client_id, api_key)
    print(f"Получено строк: {len(rows)}")

    write_to_sheet(SPREADSHEET_ID, SHEET_NAME, HEADERS, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
