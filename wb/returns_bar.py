import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Бар - Возвраты"


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
    headers = {"Authorization": f"Bearer {api_key}"}
    rows = []
    limit = 1000
    next_id = 0

    while True:
        r = requests.get(
            "https://marketplace-api.wildberries.ru/api/v3/returns",
            headers=headers,
            params={"limit": limit, "next": next_id},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка возвратов: {r.status_code} — {r.text[:300]}")
            break

        data = r.json()
        returns = data.get("returns", [])
        print(f"  Батч: {len(returns)} возвратов (next={next_id})")

        for ret in returns:
            rows.append([
                fmt_dt(ret.get("date", "")),
                ret.get("article", "") or ret.get("supplierArticle", "") or str(ret.get("nmId", "")),
                ret.get("status", ""),
                ret.get("returnReasonName", ""),
                ret.get("srid", ""),
                fmt_dt(ret.get("collectedAt", "")),
                ret.get("supplierType", "") or ret.get("type", ""),
                ret.get("orderUid", "") or ret.get("gNumber", ""),
                fmt_num(ret.get("price", "")),
            ])

        cursor = data.get("cursor", {})
        if len(returns) < limit or not cursor.get("id"):
            break
        next_id = cursor["id"]

    return rows


def main():
    api_key = os.environ["WB_BAR_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Загружаю возвраты WB Бар...")

    rows = fetch_returns(api_key)
    print(f"Возвраты: {len(rows)} строк")

    write_returns_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
