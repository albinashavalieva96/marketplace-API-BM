import json
import os
import time
import requests
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "Приход по артикулам"
DAYS_BACK = 30

CABINETS = ["WB Виз", "WB Бар", "Ozon BM", "Ozon CF", "ЯМ Виз", "ЯМ Бар"]


def fmt_money(value):
    try:
        v = round(float(value), 2)
        return str(v).replace(".", ",") if v != 0 else ""
    except (ValueError, TypeError):
        return ""


def write_sheet(income, date_from_str, date_to_str):
    """income = {cabinet: {article: {"amount": float, "qty": int}}}"""
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    n_cols = 1 + len(CABINETS) * 2
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(SHEET_NAME, rows=5000, cols=n_cols)

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))

    # Заголовки: Артикул | WB Виз шт | WB Виз за шт | WB Бар шт | WB Бар за шт | ...
    header1 = ["Артикул"]
    header2 = [""]
    for cab in CABINETS:
        header1 += [cab, ""]
        header2 += ["шт", "за 1 шт"]

    # Собираем все артикулы
    all_articles = set()
    for data in income.values():
        all_articles.update(data.keys())

    rows = []
    for article in sorted(all_articles):
        total_amount = sum(income.get(cab, {}).get(article, {}).get("amount", 0) for cab in CABINETS)
        row = [article]
        for cab in CABINETS:
            d = income.get(cab, {}).get(article, {})
            amt = d.get("amount", 0.0)
            qty = d.get("qty", 0)
            avg = round(amt / qty, 2) if qty > 0 else 0.0
            row += [qty if qty else "", fmt_money(avg)]
        rows.append((total_amount, row))

    rows.sort(key=lambda x: x[0], reverse=True)

    sheet_rows = [
        ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")] + [""] * (n_cols - 3),
        [f"Период: {date_from_str} – {date_to_str}"] + [""] * (n_cols - 1),
        [""] * n_cols,
        header1,
        header2,
    ]
    for _, row in rows:
        sheet_rows.append(row)

    ws.clear()
    ws.update("A1", sheet_rows)
    print(f"Записано: {len(rows)} артикулов → '{SHEET_NAME}'")


# ── WB ──────────────────────────────────────────────────────────────────────

def fetch_wb(api_key, cabinet_name, date_from, date_to):
    """ppvz_for_pay и quantity из детального отчёта WB."""
    result = defaultdict(lambda: {"amount": 0.0, "qty": 0})
    rrdid = 0
    params_base = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d"),
    }
    while True:
        r = requests.get(
            "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
            headers={"Authorization": f"Bearer {api_key}"},
            params={**params_base, "rrdid": rrdid},
            timeout=120,
        )
        if r.status_code == 429:
            print(f"WB {cabinet_name}: 429, жду 65 сек...")
            time.sleep(65)
            r = requests.get(
                "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
                headers={"Authorization": f"Bearer {api_key}"},
                params={**params_base, "rrdid": rrdid},
                timeout=120,
            )
        if r.status_code != 200:
            print(f"Ошибка WB {cabinet_name}: {r.status_code}")
            break
        rows = r.json()
        if not rows:
            break
        for row in rows:
            article = row.get("sa_name", "")
            if not article:
                continue
            result[article]["amount"] += float(row.get("ppvz_for_pay", 0) or 0)
            result[article]["qty"] += int(row.get("quantity", 0) or 0)
        rrdid = max(row.get("rrd_id", 0) for row in rows) + 1
        if len(rows) < 100000:
            break
    print(f"{cabinet_name}: {len(result)} артикулов")
    return result


# ── Ozon ─────────────────────────────────────────────────────────────────────

def _ozon_headers(client_id, api_key):
    return {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}


def fetch_ozon(client_id, api_key, cabinet_name, date_from, date_to):
    """customer_price и quantity из доставленных FBS заказов Ozon."""
    result = defaultdict(lambda: {"amount": 0.0, "qty": 0})
    date_from_str = date_from.strftime("%Y-%m-%dT00:00:00.000Z")
    date_to_str = date_to.strftime("%Y-%m-%dT23:59:59.999Z")
    offset = 0
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=_ozon_headers(client_id, api_key),
            json={
                "dir": "DESC",
                "filter": {"since": date_from_str, "to": date_to_str, "status": "delivered"},
                "limit": 100,
                "offset": offset,
                "with": {"analytics_data": False, "financial_data": True},
            },
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка Ozon {cabinet_name}: {r.status_code}")
            break
        postings = r.json().get("result", {}).get("postings", [])
        for posting in postings:
            fin_products = (posting.get("financial_data") or {}).get("products") or []
            for i, product in enumerate(posting.get("products", [])):
                offer_id = product.get("offer_id", "")
                qty = int(product.get("quantity", 0) or 0)
                fin = fin_products[i] if i < len(fin_products) else {}
                price = float(fin.get("customer_price", 0) or 0)
                if offer_id and qty > 0:
                    result[offer_id]["amount"] += price * qty
                    result[offer_id]["qty"] += qty
        if len(postings) < 100:
            break
        offset += 100
    print(f"{cabinet_name}: {len(result)} артикулов")
    return result


# ── ЯМ ───────────────────────────────────────────────────────────────────────

def fetch_ym(api_token, campaign_ids, cabinet_name, date_from, date_to):
    """buyerPrice и count из доставленных заказов ЯМ."""
    result = defaultdict(lambda: {"amount": 0.0, "qty": 0})
    date_from_str = date_from.strftime("%d-%m-%Y")
    date_to_str = date_to.strftime("%d-%m-%Y")
    headers = {"Api-Key": api_token}
    for campaign_id in campaign_ids:
        page = 1
        while True:
            r = requests.get(
                f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/orders",
                headers=headers,
                params={
                    "fromDate": date_from_str,
                    "toDate": date_to_str,
                    "status": "DELIVERED",
                    "limit": 50,
                    "page": page,
                },
                timeout=30,
            )
            if r.status_code != 200:
                print(f"Ошибка ЯМ {cabinet_name} кампания {campaign_id}: {r.status_code}")
                break
            data = r.json()
            result_data = data.get("result", data)
            for order in result_data.get("orders", []):
                for item in order.get("items", []):
                    offer_id = item.get("offerId", "")
                    buyer_price = float(item.get("buyerPrice", 0) or 0)
                    count = int(item.get("count", 1) or 1)
                    if offer_id:
                        result[offer_id]["amount"] += buyer_price * count
                        result[offer_id]["qty"] += count
            pager = result_data.get("pager", {})
            if page >= pager.get("pagesCount", 1):
                break
            page += 1
    print(f"{cabinet_name}: {len(result)} артикулов")
    return result


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    date_from = now - timedelta(days=DAYS_BACK)
    date_to = now

    date_from_str = date_from.strftime("%d.%m.%Y")
    date_to_str = date_to.strftime("%d.%m.%Y")

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Период: {date_from_str} – {date_to_str}")

    income = {}
    income["WB Виз"] = fetch_wb(os.environ["WB_VIZ_API_KEY"], "WB Виз", date_from, date_to)
    print("Пауза 65 сек между WB кабинетами (лимит API)...")
    time.sleep(65)
    income["WB Бар"] = fetch_wb(os.environ["WB_BAR_API_KEY"], "WB Бар", date_from, date_to)
    income["Ozon BM"] = fetch_ozon(os.environ["OZON_BM_CLIENT_ID"], os.environ["OZON_BM_API_KEY"], "Ozon BM", date_from, date_to)
    income["Ozon CF"] = fetch_ozon(os.environ["OZON_CF_CLIENT_ID"], os.environ["OZON_CF_API_KEY"], "Ozon CF", date_from, date_to)
    income["ЯМ Виз"] = fetch_ym(os.environ["YM_VIZ_API_TOKEN"], [22110675, 56291750], "ЯМ Виз", date_from, date_to)
    income["ЯМ Бар"] = fetch_ym(os.environ["YM_BAR_API_TOKEN"], [147572980], "ЯМ Бар", date_from, date_to)

    write_sheet(income, date_from_str, date_to_str)
    print("Готово!")


if __name__ == "__main__":
    main()
