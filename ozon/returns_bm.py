import os
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_ozon_returns_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - Возвраты и отмены"
DAYS_BACK = 90


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_num(value):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), 2)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def fetch_fbs_returns(client_id, api_key):
    """Все возвраты FBS (все статусы)."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows = []
    offset = 0

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/returns/company/fbs",
            headers=headers,
            json={"filter": {"status": ""}, "limit": 1000, "offset": offset},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка FBS возвраты: {r.status_code} — {r.text[:200]}")
            break
        returns = r.json().get("returns", [])
        for ret in returns:
            rows.append([
                fmt_dt(ret.get("return_date", "")),
                ret.get("offer_id", "") or str(ret.get("sku", "")),
                ret.get("status_name", ret.get("status", "")),
                ret.get("return_reason_name", ""),
                ret.get("posting_number", ""),
                "FBS возврат",
                fmt_num(ret.get("price", "")),
            ])
        if len(returns) < 1000:
            break
        offset += 1000

    return rows


def fetch_fbo_returns(client_id, api_key):
    """Все возвраты FBO (все статусы)."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows = []
    offset = 0

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v2/returns/company/fbo",
            headers=headers,
            json={"filter": {}, "limit": 1000, "offset": offset},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка FBO возвраты: {r.status_code} — {r.text[:200]}")
            break
        returns = r.json().get("returns", [])
        for ret in returns:
            rows.append([
                fmt_dt(ret.get("return_date", "")),
                ret.get("offer_id", "") or str(ret.get("sku", "")),
                ret.get("status_name", ret.get("status", "")),
                ret.get("return_reason_name", ""),
                ret.get("posting_number", ""),
                "FBO возврат",
                fmt_num(ret.get("price", "")),
            ])
        if len(returns) < 1000:
            break
        offset += 1000

    return rows


def fetch_fbs_cancelled(client_id, api_key):
    """Отменённые FBS заказы за последние DAYS_BACK дней."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []
    offset = 0

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=headers,
            json={
                "dir": "DESC",
                "filter": {"since": date_from, "to": date_to, "status": "cancelled"},
                "limit": 100,
                "offset": offset,
                "with": {"financial_data": True, "analytics_data": False},
            },
            timeout=30,
        )
        if r.status_code != 200:
            break
        postings = r.json().get("result", {}).get("postings", [])
        for p in postings:
            fin_products = (p.get("financial_data") or {}).get("products") or []
            cancel_reason = p.get("cancellation", {}).get("cancel_reason", "") if p.get("cancellation") else ""
            for i, product in enumerate(p.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                customer_price = fin.get("customer_price", "") or product.get("price", "")
                rows.append([
                    fmt_dt(p.get("in_process_at", "")),
                    product.get("offer_id", ""),
                    "Отменён",
                    cancel_reason,
                    p.get("posting_number", ""),
                    "FBS отмена",
                    fmt_num(customer_price),
                ])
        if len(postings) < 100:
            break
        offset += 100

    return rows


def fetch_fbo_cancelled(client_id, api_key):
    """Отменённые FBO заказы за последние DAYS_BACK дней."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []
    offset = 0

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v2/posting/fbo/list",
            headers=headers,
            json={
                "dir": "DESC",
                "filter": {"since": date_from, "to": date_to, "status": "cancelled"},
                "limit": 100,
                "offset": offset,
                "with": {"financial_data": True, "analytics_data": False},
            },
            timeout=30,
        )
        if r.status_code != 200:
            break
        postings = r.json().get("result", [])
        for p in postings:
            fin_products = (p.get("financial_data") or {}).get("products") or []
            for i, product in enumerate(p.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                customer_price = fin.get("customer_price", "") or product.get("price", "")
                rows.append([
                    fmt_dt(p.get("in_process_at", "")),
                    product.get("offer_id", ""),
                    "Отменён",
                    "",
                    p.get("posting_number", ""),
                    "FBO отмена",
                    fmt_num(customer_price),
                ])
        if len(postings) < 100:
            break
        offset += 100

    return rows


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    fbs_ret = fetch_fbs_returns(client_id, api_key)
    print(f"FBS возвраты: {len(fbs_ret)}")

    fbo_ret = fetch_fbo_returns(client_id, api_key)
    print(f"FBO возвраты: {len(fbo_ret)}")

    fbs_can = fetch_fbs_cancelled(client_id, api_key)
    print(f"FBS отмены (90 дней): {len(fbs_can)}")

    fbo_can = fetch_fbo_cancelled(client_id, api_key)
    print(f"FBO отмены (90 дней): {len(fbo_can)}")

    all_rows = fbs_ret + fbo_ret + fbs_can + fbo_can
    print(f"Итого: {len(all_rows)}")
    write_ozon_returns_sheet(SPREADSHEET_ID, SHEET_NAME, all_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
