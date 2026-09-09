import json
import os
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


def write_sheet(income_by_cabinet, date_from_str, date_to_str):
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(SHEET_NAME, rows=5000, cols=len(CABINETS) + 3)

    all_articles = set()
    for data in income_by_cabinet.values():
        all_articles.update(data.keys())

    rows = []
    for article in sorted(all_articles):
        total = 0.0
        row = [article]
        for cabinet in CABINETS:
            amt = income_by_cabinet.get(cabinet, {}).get(article, 0.0)
            total += amt
            row.append(fmt_money(amt))
        row.append(fmt_money(total))
        rows.append((total, row))

    rows.sort(key=lambda x: x[0], reverse=True)

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    header = ["Артикул"] + CABINETS + ["Итого"]

    sheet_rows = [
        ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")] + [""] * (len(CABINETS) - 1),
        [f"Период: {date_from_str} – {date_to_str}"] + [""] * (len(CABINETS) + 1),
        [""] * (len(CABINETS) + 2),
        header,
    ]
    for _, row in rows:
        sheet_rows.append(row)

    ws.clear()
    ws.update("A1", sheet_rows)
    print(f"Записано: {len(rows)} артикулов → '{SHEET_NAME}'")


# ── WB ──────────────────────────────────────────────────────────────────────

def fetch_wb_income(api_key, cabinet_name, date_from, date_to):
    """ppvz_for_pay из детального отчёта — фактический приход от WB."""
    result = defaultdict(float)
    rrdid = 0
    params_base = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d"),
    }
    while True:
        r = requests.get(
            "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod",
            headers={"Authorization": f"Bearer {api_key}"},
            params={**params_base, "rrdid": rrdid},
            timeout=120,
        )
        if r.status_code != 200:
            print(f"Ошибка WB {cabinet_name} отчёт: {r.status_code}")
            break
        rows = r.json()
        if not rows:
            break
        for row in rows:
            article = row.get("sa_name", "")
            amount = float(row.get("ppvz_for_pay", 0) or 0)
            if article:
                result[article] += amount
        rrdid = max(row.get("rrd_id", 0) for row in rows) + 1
        if len(rows) < 100000:
            break
    print(f"{cabinet_name}: {len(result)} артикулов, сумма {round(sum(result.values()), 2)} ₽")
    return dict(result)


# ── Ozon ─────────────────────────────────────────────────────────────────────

def _ozon_headers(client_id, api_key):
    return {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}


def _fetch_ozon_sku_map(client_id, api_key):
    now = datetime.now(timezone.utc)
    prev = (now.replace(day=1) - timedelta(days=1))
    r = requests.post(
        "https://api-seller.ozon.ru/v2/finance/realization",
        headers=_ozon_headers(client_id, api_key),
        json={"year": prev.year, "month": prev.month},
        timeout=60,
    )
    if r.status_code != 200:
        return {}
    sku_map = {}
    for row in r.json().get("result", {}).get("rows", []):
        item = row.get("item") or {}
        sku = item.get("sku")
        offer_id = item.get("offer_id", "")
        if sku and offer_id:
            sku_map[int(sku)] = offer_id
    return sku_map


def fetch_ozon_income(client_id, api_key, cabinet_name, date_from, date_to):
    """Фактические начисления из финансовых транзакций Ozon."""
    sku_map = _fetch_ozon_sku_map(client_id, api_key)
    result = defaultdict(float)
    page = 1
    while True:
        r = requests.post(
            "https://api-seller.ozon.ru/v3/finance/transaction/list",
            headers=_ozon_headers(client_id, api_key),
            json={
                "filter": {
                    "date": {
                        "from": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                        "to": date_to.strftime("%Y-%m-%dT23:59:59.999Z"),
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": 1000,
            },
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка Ozon {cabinet_name} транзакции: {r.status_code}")
            break
        ops = r.json().get("result", {}).get("operations", [])
        for op in ops:
            amount = float(op.get("amount", 0) or 0)
            items = op.get("items") or []
            if items:
                per_item = amount / len(items)
                for it in items:
                    sku = it.get("sku")
                    if sku:
                        offer_id = sku_map.get(int(sku), "")
                        if offer_id:
                            result[offer_id] += per_item
        if len(ops) < 1000:
            break
        page += 1
    print(f"{cabinet_name}: {len(result)} артикулов, сумма {round(sum(result.values()), 2)} ₽")
    return dict(result)


# ── ЯМ ───────────────────────────────────────────────────────────────────────

def fetch_ym_income(api_token, campaign_ids, cabinet_name, date_from, date_to):
    """Сумма оплаченных заказов (buyerPrice) — приближение до вычета комиссии ЯМ."""
    result = defaultdict(float)
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
                        result[offer_id] += buyer_price * count
            pager = result_data.get("pager", {})
            if page >= pager.get("pagesCount", 1):
                break
            page += 1

    print(f"{cabinet_name}: {len(result)} артикулов, сумма {round(sum(result.values()), 2)} ₽ (до вычета комиссии)")
    return dict(result)


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

    income["WB Виз"] = fetch_wb_income(os.environ["WB_VIZ_API_KEY"], "WB Виз", date_from, date_to)
    income["WB Бар"] = fetch_wb_income(os.environ["WB_BAR_API_KEY"], "WB Бар", date_from, date_to)
    income["Ozon BM"] = fetch_ozon_income(
        os.environ["OZON_BM_CLIENT_ID"], os.environ["OZON_BM_API_KEY"], "Ozon BM", date_from, date_to
    )
    income["Ozon CF"] = fetch_ozon_income(
        os.environ["OZON_CF_CLIENT_ID"], os.environ["OZON_CF_API_KEY"], "Ozon CF", date_from, date_to
    )
    # ЯМ Виз: FBY (22110675) + FBS (56291750)
    income["ЯМ Виз"] = fetch_ym_income(
        os.environ["YM_VIZ_API_TOKEN"], [22110675, 56291750], "ЯМ Виз", date_from, date_to
    )
    income["ЯМ Бар"] = fetch_ym_income(
        os.environ["YM_BAR_API_TOKEN"], [147572980], "ЯМ Бар", date_from, date_to
    )

    write_sheet(income, date_from_str, date_to_str)
    print("Готово!")


if __name__ == "__main__":
    main()
