import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - В пути"

ACTIVE_STATUSES = ["awaiting_packaging", "awaiting_deliver", "delivering"]
STATUS_RU = {
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver":   "Ожидает отгрузки",
    "delivering":         "Доставляется",
}


def _headers(client_id, api_key):
    return {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }


def fmt_money(value):
    try:
        return str(round(float(value), 2)).replace(".", ",")
    except (ValueError, TypeError):
        return "0,00"


def fetch_fbs_active(client_id, api_key):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []

    for status in ACTIVE_STATUSES:
        offset = 0
        while True:
            r = requests.post(
                "https://api-seller.ozon.ru/v3/posting/fbs/list",
                headers=_headers(client_id, api_key),
                json={
                    "dir": "DESC",
                    "filter": {"since": date_from, "to": date_to, "status": status},
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
                status_ru = STATUS_RU.get(p.get("status", ""), p.get("status", ""))
                for i, product in enumerate(p.get("products", [])):
                    fin = fin_products[i] if i < len(fin_products) else {}
                    customer_price = float(fin.get("customer_price", 0) or 0)
                    qty = product.get("quantity", 1)
                    rows.append({
                        "offer_id": product.get("offer_id", ""),
                        "qty": qty,
                        "customer_price": customer_price,
                        "total": customer_price * qty,
                        "status": status_ru,
                        "schema": "FBS",
                    })
            if len(postings) < 100:
                break
            offset += 100

    return rows


def fetch_fbo_active(client_id, api_key):
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")
    rows = []

    offset = 0
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v2/posting/fbo/list",
            headers=_headers(client_id, api_key),
            json={
                "dir": "DESC",
                "filter": {"since": date_from, "to": date_to, "status": "delivering"},
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
                customer_price = float(fin.get("customer_price", 0) or 0)
                qty = product.get("quantity", 1)
                rows.append({
                    "offer_id": product.get("offer_id", ""),
                    "qty": qty,
                    "customer_price": customer_price,
                    "total": customer_price * qty,
                    "status": "Доставляется",
                    "schema": "FBO",
                })
        if len(postings) < 100:
            break
        offset += 100

    return rows


def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_msk = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    fbs = fetch_fbs_active(client_id, api_key)
    print(f"FBS: {len(fbs)} позиций")
    fbo = fetch_fbo_active(client_id, api_key)
    print(f"FBO: {len(fbo)} позиций")

    all_rows = fbs + fbo

    # Группировка по артикулу
    by_offer = defaultdict(lambda: {"qty": 0, "total": 0.0, "statuses": set(), "schemas": set()})
    for row in all_rows:
        oid = row["offer_id"]
        by_offer[oid]["qty"] += row["qty"]
        by_offer[oid]["total"] += row["total"]
        by_offer[oid]["statuses"].add(row["status"])
        by_offer[oid]["schemas"].add(row["schema"])

    items = sorted(by_offer.items(), key=lambda x: x[1]["total"], reverse=True)
    grand_total = sum(d["total"] for _, d in items)
    print(f"Артикулов в пути: {len(items)}, итого: {round(grand_total, 2)} ₽")

    # Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=5)

    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", "", "", ""],
        ["ИТОГО:", fmt_money(grand_total) + " ₽", "", "", ""],
        ["", "", "", "", ""],
        ["Артикул", "Кол-во", "Цена клиента", "Сумма", "Статус / Схема"],
    ]

    for offer_id, d in items:
        label = ", ".join(sorted(d["statuses"])) + " · " + "/".join(sorted(d["schemas"]))
        sheet_rows.append([
            offer_id,
            d["qty"],
            fmt_money(d["total"] / d["qty"] if d["qty"] else 0) + " ₽",
            fmt_money(d["total"]) + " ₽",
            label,
        ])

    ws.clear()
    ws.update(values=sheet_rows, range_name="A1")
    print(f"Готово! → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
