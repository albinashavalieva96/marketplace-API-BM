import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Бар - Возвраты"
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


def fetch_orders_price(api_key, date_from):
    r = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": f"Bearer {api_key}"},
        params={"dateFrom": date_from, "flag": 0},
        timeout=120,
    )
    if r.status_code != 200:
        print(f"Ошибка загрузки заказов для цен: {r.status_code}")
        return {}
    return {o.get("srid", ""): o.get("finishedPrice", "") for o in r.json() if o.get("srid")}


def fetch_returns(api_key):
    rows = []
    limit = 1000
    next_id = 0

    while True:
        r = requests.get(
            "https://marketplace-api.wildberries.ru/api/v3/returns",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": limit, "next": next_id},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка возвратов: {r.status_code} — {r.text[:300]}")
            break

        data = r.json()
        returns = data.get("returns", [])
        rows.extend(returns)

        cursor = data.get("cursor", {})
        if len(returns) < limit or not cursor.get("id"):
            break
        next_id = cursor.get("id", 0)

    return rows


def main():
    api_key = os.environ["WB_BAR_API_KEY"]

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Загружаю возвраты WB Бар...")

    price_map = fetch_orders_price(api_key, date_from)
    print(f"Цены загружены: {len(price_map)} заказов")

    raw_returns = fetch_returns(api_key)
    print(f"Возвратов из API: {len(raw_returns)}")

    rows = []
    for ret in raw_returns:
        collected_at = ret.get("collectedAt", "") or ret.get("returnDate", "")
        if collected_at:
            continue

        srid = ret.get("srid", "")
        rows.append([
            fmt_dt(ret.get("date", "")),
            str(ret.get("nmId", "") or ret.get("article", "")),
            ret.get("status", ""),
            ret.get("returnReasonName", ""),
            srid,
            fmt_dt(collected_at),
            ret.get("supplierType", "") or ret.get("type", ""),
            ret.get("orderUid", ""),
            fmt_num(price_map.get(srid, "")),
        ])

    print(f"В пути к нам: {len(rows)} строк")
    write_returns_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
