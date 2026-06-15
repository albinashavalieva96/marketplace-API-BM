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


def fetch_returns(client_id, api_key):
    """Возвраты через /v1/returns/list за последние DAYS_BACK дней."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []
    offset = 0
    limit = 100

    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v1/returns/list",
            headers=headers,
            json={
                "limit": limit,
                "offset": offset,
                "filter": {
                    "return_date": {"from": date_from, "to": date_to},
                },
            },
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  Ошибка returns/list: {r.status_code} — {r.text[:200]}")
            break

        data = r.json()
        returns = data.get("returns", [])

        for ret in returns:
            product = ret.get("product") or {}
            logistic = ret.get("logistic") or {}
            visual = ret.get("visual") or {}
            status_info = visual.get("status") or {}

            schema = ret.get("schema", "")
            status_display = status_info.get("display_name", "")

            rows.append([
                fmt_dt(logistic.get("return_date", "")),
                product.get("offer_id", "") or str(product.get("sku", "")),
                status_display,
                ret.get("return_reason_name", ""),
                ret.get("posting_number", ""),
                f"{schema} возврат",
                fmt_num((product.get("price") or {}).get("price", "")),
            ])

        if not data.get("has_next"):
            break
        offset += limit

    return rows


def fetch_cancelled(client_id, api_key):
    """Отменённые FBS и FBO заказы за последние DAYS_BACK дней."""
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []

    # FBS отмены
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
            cancel_reason = (p.get("cancellation") or {}).get("cancel_reason", "")
            for i, product in enumerate(p.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                price = fin.get("customer_price", "") or product.get("price", "")
                rows.append([
                    fmt_dt(p.get("in_process_at", "")),
                    product.get("offer_id", ""),
                    "Отменён",
                    cancel_reason,
                    p.get("posting_number", ""),
                    "FBS отмена",
                    fmt_num(price),
                ])
        if len(postings) < 100:
            break
        offset += 100

    # FBO отмены
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
                price = fin.get("customer_price", "") or product.get("price", "")
                rows.append([
                    fmt_dt(p.get("in_process_at", "")),
                    product.get("offer_id", ""),
                    "Отменён",
                    "",
                    p.get("posting_number", ""),
                    "FBO отмена",
                    fmt_num(price),
                ])
        if len(postings) < 100:
            break
        offset += 100

    return rows


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    returns = fetch_returns(client_id, api_key)
    print(f"Возвраты: {len(returns)}")

    cancelled = fetch_cancelled(client_id, api_key)
    print(f"Отмены (90 дней): {len(cancelled)}")

    all_rows = returns + cancelled
    print(f"Итого: {len(all_rows)}")
    write_ozon_returns_sheet(SPREADSHEET_ID, SHEET_NAME, all_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
