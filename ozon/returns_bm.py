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
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

    base_body = {
        "dir": "DESC",
        "filter": {"since": date_from, "to": date_to, "status": ""},
        "limit": 5,
        "offset": 0,
        "with": {"analytics_data": False, "financial_data": False},
    }

    # Пробуем разные статусы возвратов
    for status in ["returned", "return_in_transit", "awaiting_return", "client_returned", "cancelled_from_customer", ""]:
        body = dict(base_body)
        body["filter"] = {"since": date_from, "to": date_to, "status": status}
        r = requests.post("https://api-seller.ozon.ru/v3/posting/fbs/list", headers=headers, json=body, timeout=30)
        postings = r.json().get("result", {}).get("postings", []) if r.status_code == 200 else []
        print(f"status='{status}' → {r.status_code}, postings={len(postings)}")
        if postings:
            print(f"  пример статус: {postings[0].get('status')}")

    # Также пробуем новый путь возвратов
    for url in [
        "https://api-seller.ozon.ru/v1/return/list",
        "https://api-seller.ozon.ru/v1/return/fbs/list",
        "https://api-seller.ozon.ru/v2/return/list",
    ]:
        r = requests.post(url, headers=headers, json={"limit": 10, "offset": 0}, timeout=30)
        print(f"POST {url} → {r.status_code}: {r.text[:100]}")


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
