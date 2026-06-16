import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_sheet, get_brand_map

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Виз - Заказы"
DAYS_BACK = 30


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
    """Возвращает множество srid заказов, которые были доставлены (saleID начинается с S)."""
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


def fetch_orders(api_key, brand_map, delivered_srids):
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
        if o.get("isCancel"):
            status = "Отменён"
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

    delivered_srids = fetch_delivered_srids(api_key, date_from)
    print(f"Доставленных заказов: {len(delivered_srids)}")

    rows = fetch_orders(api_key, brand_map, delivered_srids)
    print(f"Заказы: {len(rows)} строк")

    write_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
