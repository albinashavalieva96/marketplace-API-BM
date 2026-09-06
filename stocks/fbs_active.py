import json
import os
import sys
import requests
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "FBS в работе"
DAYS_BACK = 30

HEADERS = ["Кабинет", "Номер отправления", "Артикул", "Количество", "Статус", "Дата создания"]

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
WB_INACTIVE = {"sold", "canceled", "canceled_by_client", "defect"}

OZON_STATUS_RU = {
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
    "delivering": "Доставляется",
    "delivered": "Доставлено",
    "cancelled": "Отменено",
    "not_accepted": "Не принято на сортировке",
}
OZON_INACTIVE = {"delivered", "cancelled"}

YM_STATUS_RU = {
    "CANCELLED": "Отменено",
    "DELIVERED": "Доставлено",
    "DELIVERY": "Доставляется",
    "PICKUP": "Пункт выдачи",
    "PROCESSING": "В обработке",
    "PENDING": "Ожидает подтверждения",
    "UNPAID": "Ожидает оплаты",
    "CANCELLED_IN_DELIVERY": "Отменен при доставке",
}
YM_INACTIVE = {"CANCELLED", "DELIVERED", "CANCELLED_IN_DELIVERY"}


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
        ws = spreadsheet.add_worksheet(SHEET_NAME, rows=1000, cols=len(HEADERS))

    all_rows = [HEADERS] + rows
    ws.resize(rows=max(len(all_rows), 1), cols=len(HEADERS))
    ws.update("A1", all_rows)
    print(f"Записано: {len(rows)} строк → '{SHEET_NAME}'")


def _wb_fbs_statuses(api_key):
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
            break
        data = r.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)
        next_cursor = data.get("next", 0)
        if len(orders) < 1000 or not next_cursor:
            break

    if not all_orders:
        return {}

    status_map = {}
    for i in range(0, len(all_orders), 1000):
        batch = all_orders[i:i + 1000]
        uid_by_id = {o["id"]: o["orderUid"] for o in batch}
        r = requests.post(
            "https://marketplace-api.wildberries.ru/api/v3/orders/status",
            headers=headers,
            json={"orders": list(uid_by_id.keys())},
            timeout=60,
        )
        if r.status_code != 200:
            continue
        for s in r.json().get("orders", []):
            uid = uid_by_id.get(s["id"], "")
            if uid:
                status_map[uid] = s.get("wbStatus", "")
    return status_map


def fetch_wb(api_key, cabinet_name):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    fbs_statuses = _wb_fbs_statuses(api_key)

    r_sales = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=120,
    )
    delivered_srids = set()
    if r_sales.status_code == 200:
        delivered_srids = {
            s["srid"] for s in r_sales.json()
            if str(s.get("saleID", "")).startswith("S") and s.get("srid")
        }

    r_orders = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=120,
    )
    if r_orders.status_code != 200:
        print(f"Ошибка WB {cabinet_name}: {r_orders.status_code}")
        return []

    rows = []
    for o in r_orders.json():
        if o.get("warehouseType") == "Склад WB":
            continue
        if o.get("isCancel"):
            continue
        srid = o.get("srid", "")
        if srid in delivered_srids:
            continue
        parts = srid.split(".")
        order_uid = parts[1] if len(parts) > 1 else ""
        wb_status = fbs_statuses.get(order_uid, "")
        if wb_status in WB_INACTIVE:
            continue
        status = WB_STATUS_RU.get(wb_status, "В работе") if wb_status else "В работе"
        rows.append([
            cabinet_name,
            srid,
            o.get("supplierArticle", ""),
            o.get("quantity") or 1,
            status,
            fmt_dt(o.get("date", "")),
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
            if posting.get("status", "") in OZON_INACTIVE:
                continue
            status = OZON_STATUS_RU.get(posting.get("status", ""), posting.get("status", ""))
            for product in posting.get("products", []):
                rows.append([
                    cabinet_name,
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
            if status in YM_INACTIVE:
                continue
            status_ru = YM_STATUS_RU.get(status, status)
            for item in order.get("items", []):
                rows.append([
                    cabinet_name,
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
