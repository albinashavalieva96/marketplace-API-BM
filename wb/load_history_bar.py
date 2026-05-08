import os
import sys
import requests
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Бар - Заказы"

# Период для исторической загрузки
DATE_FROM = "2025-06-01T00:00:00"
DATE_TO   = "2025-09-01T00:00:00"


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


def fetch_orders(api_key):
    r = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": DATE_FROM, "flag": 1},
        timeout=120,
    )

    if r.status_code != 200:
        print(f"Ошибка WB: {r.status_code} — {r.text}")
        return []

    rows = []
    for o in r.json():
        order_date = o.get("date", "")
        if order_date and order_date >= DATE_TO:
            continue
        status = "Отменено" if o.get("isCancel") else "В работе"
        supply_type = "FBO" if o.get("warehouseType") == "Склад WB" else "FBS"
        rows.append([
            o.get("gNumber", ""),
            o.get("srid", ""),
            fmt_dt(order_date),
            fmt_dt(o.get("lastChangeDate", "")),
            status,
            o.get("supplierArticle", ""),
            fmt_num(o.get("totalPrice", "")),
            o.get("quantity", 0),
            o.get("warehouseName", ""),
            o.get("oblastOkrugName", ""),
            fmt_num(o.get("finishedPrice", "")),
            fmt_spp(o.get("spp", "")),
            supply_type,
        ])

    return rows


def main():
    api_key = os.environ["WB_BAR_API_KEY"]

    print(f"Исторический период: {DATE_FROM[:10]} — {DATE_TO[:10]}")

    rows = fetch_orders(api_key)
    print(f"Заказы: {len(rows)} строк")

    write_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
