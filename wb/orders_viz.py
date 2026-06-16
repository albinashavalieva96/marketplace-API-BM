import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_sheet, get_brand_map

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Виз - Заказы"
DAYS_BACK = 30

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


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def fmt_spp(value):
    try:
        return str(round(float(value) / 100, 10)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def fmt_date(value):
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value)[:19])
        return dt.strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        return str(value)[:10]


def fetch_delivered_srids(api_key, date_from):
    """srid FBO-заказов, которые уже доставлены (есть в продажах с saleID=S*)."""
    r = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"Ошибка загрузки продаж: {r.status_code}")
        return set()
    return {s["srid"] for s in r.json() if str(s.get("saleID", "")).startswith("S") and s.get("srid")}


def fetch_fbs_statuses(api_key):
    """Возвращает {orderUid: wbStatus} для всех FBS-заказов через маркетплейс API."""
    headers = {"Authorization": f"Bearer {api_key}"}
    all_orders = []
    next_cursor = 0

    for _ in range(20):  # max 20 000 заказов
        r = requests.get(
            "https://marketplace-api.wildberries.ru/api/v3/orders",
            headers=headers,
            params={"limit": 1000, "next": next_cursor},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка /api/v3/orders: {r.status_code}")
            break
        data = r.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)
        next_cursor = data.get("next", 0)
        if len(orders) < 1000 or not next_cursor:
            break

    if not all_orders:
        return {}

    print(f"FBS заказов из маркетплейс API: {len(all_orders)}")

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
            print(f"Ошибка /api/v3/orders/status: {r.status_code}")
            continue
        for s in r.json().get("orders", []):
            uid = uid_by_id.get(s["id"], "")
            if uid:
                status_map[uid] = s.get("wbStatus", "")

    return status_map


def fetch_orders(api_key, brand_map, fbs_statuses, delivered_srids):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    r = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"Ошибка WB: {r.status_code} — {r.text}")
        return []

    rows = []
    for o in r.json():
        srid = o.get("srid", "")
        # srid формат: "xxx.{orderUid}.n.n" — извлекаем orderUid
        parts = srid.split(".")
        order_uid = parts[1] if len(parts) > 1 else ""
        wb_status = fbs_statuses.get(order_uid, "")

        if o.get("isCancel"):
            status = "Отменён"
        elif wb_status:
            status = WB_STATUS_RU.get(wb_status, wb_status)
        elif srid in delivered_srids:
            status = "Доставлен"
        else:
            status = "В работе"

        supply_type = "FBO" if o.get("warehouseType") == "Склад WB" else "FBS"
        article = o.get("supplierArticle", "")
        rows.append([
            o.get("gNumber", ""),
            srid,
            fmt_dt(o.get("date", "")),
            fmt_dt(o.get("lastChangeDate", "")),
            status,
            article,
            fmt_num(o.get("totalPrice", "")),
            o.get("quantity") or 1,
            o.get("warehouseName", ""),
            o.get("oblastOkrugName", ""),
            fmt_num(o.get("finishedPrice", "")),
            fmt_spp(o.get("spp", "")),
            supply_type,
            fmt_date(o.get("date", "")),
            brand_map.get(article, ""),
        ])

    return rows


def main():
    api_key = os.environ["WB_VIZ_API_KEY"]
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю заказы WB Виз за последние {DAYS_BACK} дней...")

    brand_map = get_brand_map(SPREADSHEET_ID)
    print(f"Справочник брендов: {len(brand_map)} артикулов")

    fbs_statuses = fetch_fbs_statuses(api_key)
    print(f"FBS статусов загружено: {len(fbs_statuses)}")

    date_from_sales = (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
    delivered_srids = fetch_delivered_srids(api_key, date_from_sales)
    print(f"FBO доставленных: {len(delivered_srids)}")

    rows = fetch_orders(api_key, brand_map, fbs_statuses, delivered_srids)
    print(f"Заказы: {len(rows)} строк")

    write_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
