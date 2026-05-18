import os
import sys
import requests
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_ozon_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - Возвраты"

# Статусы "уже у продавца/отменён" — исключаем
DONE_STATUSES = {"returned_to_seller", "cancelled"}


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def probe_endpoints(client_id, api_key):
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    post_candidates = [
        ("https://api-seller.ozon.ru/v1/returns", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v2/returns", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v1/returns/company/fbs", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v2/returns/company/fbs", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v1/posting/fbs/returns", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v3/posting/returns", {"filter": {}, "limit": 10, "offset": 0}),
        ("https://api-seller.ozon.ru/v1/finance/returns", {"filter": {}, "limit": 10, "offset": 0}),
    ]
    for url, body in post_candidates:
        r = requests.post(url, headers=headers, json=body, timeout=30)
        print(f"POST {url} → {r.status_code}: {r.text[:100]}")

    get_candidates = [
        "https://api-seller.ozon.ru/v1/returns",
        "https://api-seller.ozon.ru/v2/returns",
        "https://api-seller.ozon.ru/v1/returns/company/fbs",
    ]
    for url in get_candidates:
        r = requests.get(url, headers=headers, timeout=30)
        print(f"GET  {url} → {r.status_code}: {r.text[:100]}")


def fetch_fbs_returns(client_id, api_key):
    return []


def fetch_fbo_returns(client_id, api_key):
    return []


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    probe_endpoints(client_id, api_key)

    fbs_rows = fetch_fbs_returns(client_id, api_key)
    print(f"FBS возвраты в пути: {len(fbs_rows)}")

    fbo_rows = fetch_fbo_returns(client_id, api_key)
    print(f"FBO возвраты в пути: {len(fbo_rows)}")

    all_rows = fbs_rows + fbo_rows
    print(f"Итого: {len(all_rows)}")
    write_ozon_returns_sheet(SPREADSHEET_ID, SHEET_NAME, all_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
