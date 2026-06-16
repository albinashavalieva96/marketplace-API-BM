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


def fetch_returns(api_key):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    rows = []
    page = 1

    while True:
        r = requests.get(
            "https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-return",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"dateFrom": date_from, "dateTo": date_to, "page": page},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка goods-return: {r.status_code} — {r.text[:300]}")
            break

        records = r.json().get("report", [])
        print(f"  Страница {page}: {len(records)} записей")
        if not records:
            break

        for rec in records:
            rows.append([
                fmt_dt(rec.get("orderDt", "")),
                str(rec.get("nmId", "")),
                rec.get("subjectName", ""),
                rec.get("status", ""),
                rec.get("returnType", ""),
                rec.get("reason", ""),
                rec.get("srid", ""),
                fmt_dt(rec.get("readyToReturnDt", "")),
                fmt_dt(rec.get("completedDt", "")),
                rec.get("stickerId", ""),
            ])

        page += 1

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
