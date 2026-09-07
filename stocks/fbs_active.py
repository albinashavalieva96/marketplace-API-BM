import json
import os
import requests
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "FBS в работе"
DAYS_BACK = 30

HEADERS = ["Кабинет", "Номер заказа", "Номер отправления", "Артикул", "Количество", "Статус", "Дата создания"]

WB_STATUS_RU = {
    "waiting": "Ожидает",
    "sorted": "Отправлен",
    "ready_for_pickup": "В пункте выдачи",
    "sold": "Доставлен",
    "canceled": "Отменён",
    "canceled_by_client": "Отменён покупателем",
    "defect": "Брак",
    "part_delivered_by_client": "Частично доставлен",
}
# Только "waiting" нужно собирать; sorted/ready_for_pickup уже едут
WB_ACTIVE = {"waiting"}

OZON_STATUS_RU = {
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
}
# delivering/delivered/cancelled/not_accepted — не показываем
OZON_ACTIVE = set(OZON_STATUS_RU.keys())

YM_STATUS_RU = {
    "PROCESSING": "В обработке",
    "PENDING": "Ожидает подтверждения",
    "UNPAID": "Ожидает оплаты",
}
# DELIVERY/PICKUP/DELIVERED/CANCELLED — не показываем
YM_ACTIVE = set(YM_STATUS_RU.keys())


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_ym_dt(value):
    if not value:
        return ""
    try:
        parts = str(value).split(" ")
        d, m, y = parts[0].split("-")
        result = f"{y}-{m}-{d}"
        if len(parts) > 1:
            result += f" {parts[1]}"
        return result
    except Exception:
        return str(value)


def write_sheet(rows):
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(SHEET_NAME, rows=1000, cols=len(HEADERS) + 1)

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    service = ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")]

    header_row = [""] + HEADERS
    all_rows = [header_row]
    for i, data_row in enumerate(rows):
        service_cell = service[i] if i < len(service) else ""
        all_rows.append([service_cell] + list(data_row))

    ws.resize(rows=max(len(all_rows), 1), cols=len(HEADERS) + 1)
    ws.update("A1", all_rows)
    print(f"Записано: {len(rows)} строк → '{SHEET_NAME}'")


def fetch_wb(api_key, cabinet_name):
    """Активные FBS заказы напрямую из Marketplace API (реальное время)."""
    headers = {"Authorization": f"Bearer {api_key}"}
    all_orders = []
    next_cursor = 0

    for _ in range(20):
        r = requests.get(
            "https://marketplace-api.wildberries.ru/api/v3/orders",
            headers=headers,
            params={"limit": 1000, "next": next_cursor},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка WB {cabinet_name} /orders: {r.status_code}")
            break
        data = r.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)
        next_cursor = data.get("next", 0)
        if len(orders) < 1000 or not next_cursor:
            break

    if not all_orders:
        print(f"{cabinet_name}: 0 заказов")
        return []

    status_map = {}
    for i in range(0, len(all_orders), 1000):
        batch = all_orders[i:i + 1000]
        ids = [o["id"] for o in batch]
        r = requests.post(
            "https://marketplace-api.wildberries.ru/api/v3/orders/status",
            headers=headers,
            json={"orders": ids},
            timeout=60,
        )
        if r.status_code != 200:
            continue
        for s in r.json().get("orders", []):
            status_map[s["id"]] = s.get("wbStatus", "")

    rows = []
    for o in all_orders:
        wb_status = status_map.get(o["id"], "")
        if wb_status not in WB_ACTIVE:
            continue
        status = WB_STATUS_RU.get(wb_status, "Ожидает")
        rows.append([
            cabinet_name,
            str(o.get("id", "")),
            o.get("orderUid", ""),
            o.get("article", ""),
            1,
            status,
            fmt_dt(o.get("createdAt", "")),
        ])

    print(f"{cabinet_name}: {len(rows)} активных FBS")
    return rows


def fetch_ozon(client_id, api_key, cabinet_name):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    rows = []
    offset = 0
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=headers,
            json={
                "dir": "DESC",
                "filter": {"since": date_from, "to": date_to, "status": ""},
                "limit": 100,
                "offset": offset,
                "with": {"analytics_data": False, "financial_data": False},
            },
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка Ozon {cabinet_name}: {r.status_code}")
            break
        postings = r.json().get("result", {}).get("postings", [])
        for posting in postings:
            if posting.get("status", "") not in OZON_ACTIVE:
                continue
            status = OZON_STATUS_RU.get(posting.get("status", ""), posting.get("status", ""))
            for product in posting.get("products", []):
                rows.append([
                    cabinet_name,
                    posting.get("order_number", ""),
                    posting.get("posting_number", ""),
                    product.get("offer_id", ""),
                    product.get("quantity", 0),
                    status,
                    fmt_dt(posting.get("in_process_at", "")),
                ])
        if len(postings) < 100:
            break
        offset += 100

    print(f"{cabinet_name}: {len(rows)} активных FBS")
    return rows


def fetch_ym(api_token, campaign_id, cabinet_name):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%d-%m-%Y")
    date_to = now.strftime("%d-%m-%Y")
    headers = {"Api-Key": api_token}
    rows = []
    page = 1
    while True:
        r = requests.get(
            f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/orders",
            headers=headers,
            params={"fromDate": date_from, "toDate": date_to, "limit": 50, "page": page},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка ЯМ {cabinet_name}: {r.status_code}")
            break
        data = r.json()
        result = data.get("result", data)
        orders = result.get("orders", [])
        for order in orders:
            status = order.get("status", "")
            if status not in YM_ACTIVE:
                continue
            status_ru = YM_STATUS_RU.get(status, status)
            for item in order.get("items", []):
                rows.append([
                    cabinet_name,
                    str(order.get("id", "")),
                    f"{order.get('id', '')}_{item.get('id', '')}",
                    item.get("offerId", ""),
                    item.get("count", 0),
                    status_ru,
                    fmt_ym_dt(order.get("creationDate", "")),
                ])
        pager = result.get("pager", {})
        if page >= pager.get("pagesCount", 1):
            break
        page += 1

    print(f"{cabinet_name}: {len(rows)} активных FBS")
    return rows


def main():
    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    all_rows = []

    all_rows.extend(fetch_wb(os.environ["WB_VIZ_API_KEY"], "WB Виз"))
    all_rows.extend(fetch_wb(os.environ["WB_BAR_API_KEY"], "WB Бар"))
    all_rows.extend(fetch_ozon(os.environ["OZON_BM_CLIENT_ID"], os.environ["OZON_BM_API_KEY"], "Ozon BM"))
    all_rows.extend(fetch_ozon(os.environ["OZON_CF_CLIENT_ID"], os.environ["OZON_CF_API_KEY"], "Ozon CF"))
    all_rows.extend(fetch_ym(os.environ["YM_VIZ_API_TOKEN"], 56291750, "ЯМ Виз"))
    all_rows.extend(fetch_ym(os.environ["YM_BAR_API_TOKEN"], 147572980, "ЯМ Бар"))

    all_rows.sort(key=lambda r: (r[2], r[0]))

    print(f"Итого активных FBS: {len(all_rows)}")
    write_sheet(all_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
