import os
import sys
import requests
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_ozon_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon CF - Возвраты"

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


def fetch_fbs_returns(client_id, api_key):
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows = []
    offset = 0
    limit = 1000

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/returns/company/fbs",
            headers=headers,
            json={"filter": {"status": ""}, "limit": limit, "offset": offset},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка FBS возвраты: {r.status_code} — {r.text[:200]}")
            break

        returns = r.json().get("returns", [])
        for ret in returns:
            if ret.get("status", "") in DONE_STATUSES:
                continue
            rows.append([
                fmt_dt(ret.get("return_date", "")),
                ret.get("offer_id", "") or str(ret.get("sku", "")),
                ret.get("status_name", ret.get("status", "")),
                ret.get("return_reason_name", ""),
                ret.get("posting_number", ""),
                "FBS",
                fmt_num(ret.get("price", "")),
            ])

        if len(returns) < limit:
            break
        offset += limit

    return rows


def fetch_fbo_returns(client_id, api_key):
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows = []
    offset = 0
    limit = 1000

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v2/returns/company/fbo",
            headers=headers,
            json={"filter": {}, "limit": limit, "offset": offset},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка FBO возвраты: {r.status_code} — {r.text[:200]}")
            break

        returns = r.json().get("returns", [])
        for ret in returns:
            if ret.get("status", "") in DONE_STATUSES:
                continue
            rows.append([
                fmt_dt(ret.get("return_date", "")),
                ret.get("offer_id", "") or str(ret.get("sku", "")),
                ret.get("status_name", ret.get("status", "")),
                ret.get("return_reason_name", ""),
                ret.get("posting_number", ""),
                "FBO",
                fmt_num(ret.get("price", "")),
            ])

        if len(returns) < limit:
            break
        offset += limit

    return rows


def main():
    client_id = os.environ["OZON_CF_CLIENT_ID"]
    api_key = os.environ["OZON_CF_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
