import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Виз - Возвраты"
DAYS_BACK = 90


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def fetch_returns(api_key):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    r = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/returns",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=120,
    )
    if r.status_code != 200:
        print(f"Ошибка WB returns: {r.status_code} — {r.text[:300]}")
        return []

    rows = []
    for ret in r.json():
        supply_type = "FBO" if ret.get("warehouseType") == "Склад WB" else "FBS"
        rows.append([
            fmt_dt(ret.get("date", "")),
            ret.get("supplierArticle", ""),
            "",
            "",
            ret.get("srid", ""),
            "",
            supply_type,
            ret.get("gNumber", ""),
            fmt_num(ret.get("finishedPrice", "")),
        ])

    return rows


def main():
    api_key = os.environ["WB_VIZ_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю возвраты WB Виз за последние {DAYS_BACK} дней...")

    rows = fetch_returns(api_key)
    print(f"Возвраты: {len(rows)} строк")

    write_returns_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
