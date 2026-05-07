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
    "Причина отмены",
    "Дата создания",
    "Дата начала обработки",
    "Дата отгрузки (FBS)",
    "Артикул продавца",
    "SKU Ozon",
    "Название товара",
    "Количество",
    "Цена",
    "Старая цена",
    "Скидка (сумма)",
    "Скидка (%)",
    "Регион доставки",
    "Город доставки",
    "Способ доставки",
    "Название склада",
    "Оплачено покупателем",
    "Комиссия Ozon (сумма)",
    "Комиссия Ozon (%)",
    "Выплата продавцу",
    "Стоимость фулфилмента",
    "Стоимость доставки",
    "Стоимость возврата",
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
                    "financial_data": True,
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
                financial = posting.get("financial_data") or {}
                fin_products = financial.get("products") or []
                cancellation = posting.get("cancellation") or {}

                status_raw = posting.get("status", "")
                status = STATUS_MAP.get(status_raw, status_raw)
                cancel_reason = cancellation.get("cancel_reason", "")
                shipment_date = fmt_dt(posting.get("shipment_date", "")) if schema == "fbs" else ""

                products = posting.get("products", [])
                for i, product in enumerate(products):
                    fin = fin_products[i] if i < len(fin_products) else {}
                    services = fin.get("item_services") or {}

                    row = [
                        schema.upper(),
                        posting.get("posting_number", ""),
                        posting.get("order_number", ""),
                        status,
                        cancel_reason,
                        fmt_dt(posting.get("created_at", "")),
                        fmt_dt(posting.get("in_process_at", "")),
                        shipment_date,
                        product.get("offer_id", ""),
                        product.get("sku", ""),
                        product.get("name", ""),
                        product.get("quantity", 0),
                        product.get("price", ""),
                        fin.get("old_price", ""),
                        fin.get("total_discount_value", ""),
                        fin.get("total_discount_percent", ""),
                        analytics.get("region", ""),
                        analytics.get("city", ""),
                        analytics.get("delivery_type", ""),
                        analytics.get("warehouse_name", ""),
                        fin.get("client_price", ""),
                        fin.get("commission_amount", ""),
                        fin.get("commission_percent", ""),
                        fin.get("payout", ""),
                        services.get("marketplace_service_item_fulfillment", ""),
                        services.get("marketplace_service_item_direct_flow_trans", ""),
                        services.get("marketplace_service_item_return_flow_trans", ""),
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
