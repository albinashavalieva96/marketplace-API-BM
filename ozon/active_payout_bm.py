import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - Ozon BM - В пути (ожидаемые)"

ACTIVE_FBS = ["awaiting_packaging", "awaiting_deliver", "delivering"]
ACTIVE_FBO = ["delivering"]

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


# ── Реализация: средняя чистая выплата за штуку по артикулу ─────────────────

def fetch_payout_per_unit(client_id, api_key):
    now = datetime.now(timezone.utc)
    prev = now.replace(day=1) - timedelta(days=1)

    r = requests.post(
        "https://api-seller.ozon.ru/v2/finance/realization",
        headers=_headers(client_id, api_key),
        json={"year": prev.year, "month": prev.month},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"  realization error: {r.status_code}")
        return {}

    rows = r.json().get("result", {}).get("rows", [])
    totals = defaultdict(lambda: {"net": 0.0, "qty": 0})

    for row in rows:
        offer_id = (row.get("item") or {}).get("offer_id", "")
        if not offer_id:
            continue
        dc = row.get("delivery_commission") or {}
        net = (float(dc.get("total", 0) or 0)
               - float(dc.get("amount", 0) or 0)
               + float(dc.get("bonus", 0) or 0))
        qty = int(dc.get("quantity", 1) or 1)
        totals[offer_id]["net"] += net
        totals[offer_id]["qty"] += qty

    result = {}
    for offer_id, d in totals.items():
        if d["qty"] > 0:
            result[offer_id] = d["net"] / d["qty"]

    print(f"  Артикулов в реализации: {len(result)}")
    return result


# ── FBS: активные заказы ─────────────────────────────────────────────────────

def fetch_fbs_active(client_id, api_key):
    headers = _headers(client_id, api_key)
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

    rows = []
    for status in ACTIVE_FBS:
        offset = 0
        while True:
            r = requests.post(
                "https://api-seller.ozon.ru/v3/posting/fbs/list",
                headers=headers,
                json={
                    "dir": "DESC",
                    "filter": {"since": date_from, "to": date_to, "status": status},
                    "limit": 100,
                    "offset": offset,
                    "with": {"financial_data": False, "analytics_data": False},
                },
                timeout=30,
            )
            if r.status_code != 200:
                break
            postings = r.json().get("result", {}).get("postings", [])
            for p in postings:
                status_ru = STATUS_RU.get(p.get("status", ""), p.get("status", ""))
                for product in p.get("products", []):
                    rows.append({
                        "offer_id": product.get("offer_id", ""),
                        "qty": product.get("quantity", 1),
                        "status": status_ru,
                        "schema": "FBS",
                        "posting": p.get("posting_number", ""),
                    })
            if len(postings) < 100:
                break
            offset += 100

    return rows


# ── FBO: активные заказы ─────────────────────────────────────────────────────

def fetch_fbo_active(client_id, api_key):
    headers = _headers(client_id, api_key)
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000Z")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999Z")

    rows = []
    for status in ACTIVE_FBO:
        offset = 0
        while True:
            r = requests.post(
                "https://api-seller.ozon.ru/v2/posting/fbo/list",
                headers=headers,
                json={
                    "dir": "DESC",
                    "filter": {"since": date_from, "to": date_to, "status": status},
                    "limit": 100,
                    "offset": offset,
                    "with": {"financial_data": False, "analytics_data": False},
                },
                timeout=30,
            )
            if r.status_code != 200:
                break
            postings = r.json().get("result", [])
            for p in postings:
                status_ru = STATUS_RU.get(p.get("status", ""), p.get("status", ""))
                for product in p.get("products", []):
                    rows.append({
                        "offer_id": product.get("offer_id", ""),
                        "qty": product.get("quantity", 1),
                        "status": status_ru,
                        "schema": "FBO",
                        "posting": p.get("posting_number", ""),
                    })
            if len(postings) < 100:
                break
            offset += 100

    return rows


# ── Основная логика ───────────────────────────────────────────────────────────

def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    now_msk = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    print(f"Запуск: {now_msk.strftime('%Y-%m-%d %H:%M:%S')}")

    print("Реализация (выплата за штуку):")
    payout_per_unit = fetch_payout_per_unit(client_id, api_key)

    print("FBS активные заказы:")
    fbs_rows = fetch_fbs_active(client_id, api_key)
    print(f"  Позиций: {len(fbs_rows)}")

    print("FBO активные заказы:")
    fbo_rows = fetch_fbo_active(client_id, api_key)
    print(f"  Позиций: {len(fbo_rows)}")

    all_rows = fbs_rows + fbo_rows

    # Группировка по артикулу
    by_offer = defaultdict(lambda: {"qty": 0, "statuses": set(), "schemas": set()})
    for row in all_rows:
        oid = row["offer_id"]
        by_offer[oid]["qty"] += row["qty"]
        by_offer[oid]["statuses"].add(row["status"])
        by_offer[oid]["schemas"].add(row["schema"])

    # Расчёт ожидаемой выплаты
    items = []
    total_expected = 0.0
    no_rate_count = 0

    for offer_id, data in by_offer.items():
        qty = data["qty"]
        rate = payout_per_unit.get(offer_id)
        if rate is not None:
            expected = qty * rate
        else:
            expected = 0.0
            no_rate_count += 1
        total_expected += expected
        items.append({
            "offer_id": offer_id,
            "qty": qty,
            "rate": rate,
            "expected": expected,
            "statuses": ", ".join(sorted(data["statuses"])),
            "schemas": "/".join(sorted(data["schemas"])),
        })

    items.sort(key=lambda x: x["expected"], reverse=True)
    print(f"Артикулов в пути: {len(items)}, без ставки: {no_rate_count}")
    print(f"Ожидаемая выплата итого: {round(total_expected, 2)} ₽")

    # Запись в Google Sheets
    sheets_client = get_sheets_client()
    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=6)

    sheet_rows = [
        [f"Обновлен: {now_msk.strftime('%Y-%m-%d %H:%M')}", "", "", "", "", ""],
        [f"ИТОГО ожидаемая выплата:", fmt_money(total_expected) + " ₽", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["Артикул", "Кол-во", "Выплата за шт.", "Ожидаемая выплата", "Статус(ы)", "Схема"],
    ]

    for it in items:
        rate_str = fmt_money(it["rate"]) + " ₽" if it["rate"] is not None else "нет данных"
        expected_str = fmt_money(it["expected"]) + " ₽" if it["expected"] else "—"
        sheet_rows.append([
            it["offer_id"],
            it["qty"],
            rate_str,
            expected_str,
            it["statuses"],
            it["schemas"],
        ])

    ws.clear()
    ws.update(values=sheet_rows, range_name="A1")
    print(f"Готово! → '{SHEET_NAME}'")


if __name__ == "__main__":
    main()
