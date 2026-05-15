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


def fetch_orders_price(api_key, date_from):
    """Строим словарь srid → finishedPrice из API заказов."""
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
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00Z")

    get_candidates = [
        ("https://marketplace-api.wildberries.ru/api/v3/returns/company/unredeemed", {"limit": 10}),
        ("https://marketplace-api.wildberries.ru/api/v3/supply-requests", {"limit": 10}),
        ("https://marketplace-api.wildberries.ru/api/v3/nm/returns", {"limit": 10}),
    ]
    for url, params in get_candidates:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"GET {url} → {r.status_code}: {r.text[:150]}")

    post_candidates = [
        ("https://marketplace-api.wildberries.ru/api/v3/returns", {"dateFrom": date_from, "limit": 10}),
        ("https://statistics-api.wildberries.ru/api/v1/supplier/returns", {"dateFrom": date_from}),
    ]
    for url, body in post_candidates:
        r = requests.post(url, headers=headers, json=body, timeout=30)
        print(f"POST {url} → {r.status_code}: {r.text[:150]}")

    return []


def main():
    api_key = os.environ["WB_VIZ_API_KEY"]

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00")

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Загружаю возвраты WB Виз...")

    price_map = fetch_orders_price(api_key, date_from)
    print(f"Цены загружены: {len(price_map)} заказов")

    raw_returns = fetch_returns(api_key)
    print(f"Возвратов из API: {len(raw_returns)}")
    if raw_returns:
        print(f"Пример записи: {raw_returns[0]}")

    rows = []
    for ret in raw_returns:
        collected_at = ret.get("collectedAt", "") or ret.get("returnDate", "")
        # Только те, что ещё не забрали (товар в пути к нам)
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
