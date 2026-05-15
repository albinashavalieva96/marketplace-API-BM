import os
import io
import csv
import sys
import time
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import merge_and_write, DATA_HEADERS

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
FBS_SHEET = "API - Ozon BM - Заказы FBS"
FBO_SHEET = "API - Ozon BM - Заказы FBO"

# Период для исторической загрузки
DATE_FROM = "2025-06-01T00:00:00.000Z"
DATE_TO   = "2025-09-01T23:59:59.999Z"

STATUS_MAP = {
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
    "delivering": "Доставляется",
    "delivered": "Доставлено",
    "cancelled": "Отменено",
    "not_accepted": "Не принято на сортировке",
}


def fmt_dt(value):
    if not value:
        return ""
    return value[:19].replace("T", " ")


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def calc_spp(price, customer_price):
    try:
        p = float(str(price).replace(",", "."))
        cp = float(str(customer_price).replace(",", "."))
        return str(round((p - cp) / p, 10)).replace(".", ",") if cp and p else ""
    except (ValueError, TypeError):
        return ""


def fetch_fbs(client_id, api_key):
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }

    all_rows = []
    offset = 0
    limit = 100

    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": DATE_FROM, "to": DATE_TO, "status": ""},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": True},
        }
        r = requests.post(
            "https://api-seller.ozon.ru/v3/posting/fbs/list",
            headers=headers, json=payload, timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка FBS: {r.status_code} — {r.text}")
            break

        postings = r.json().get("result", {}).get("postings", [])
        for posting in postings:
            financial = posting.get("financial_data") or {}
            fin_products = financial.get("products") or []
            status = STATUS_MAP.get(posting.get("status", ""), posting.get("status", ""))
            for i, product in enumerate(posting.get("products", [])):
                fin = fin_products[i] if i < len(fin_products) else {}
                price = product.get("price", "")
                customer_price = fin.get("customer_price", "")
                all_rows.append([
                    posting.get("order_number", ""),
                    posting.get("posting_number", ""),
                    fmt_dt(posting.get("in_process_at", "")),
                    fmt_dt(posting.get("shipment_date", "")),
                    status,
                    product.get("offer_id", ""),
                    fmt_num(price),
                    product.get("quantity", 0),
                    financial.get("cluster_from", ""),
                    financial.get("cluster_to", ""),
                    fmt_num(customer_price),
                    calc_spp(price, customer_price),
                ])

        if len(postings) < limit:
            break
        offset += limit

    return all_rows


def fetch_fbo_clusters(client_id, api_key):
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    clusters = {}
    offset = 0
    limit = 100

    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": DATE_FROM, "to": DATE_TO, "status": ""},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": True},
        }
        r = requests.post(
            "https://api-seller.ozon.ru/v2/posting/fbo/list",
            headers=headers, json=payload, timeout=30,
        )
        if r.status_code != 200:
            break
        postings = r.json().get("result", [])
        for p in postings:
            fin = p.get("financial_data") or {}
            clusters[p.get("posting_number", "")] = (
                fin.get("cluster_from", ""),
                fin.get("cluster_to", ""),
            )
        if len(postings) < limit:
            break
        offset += limit

    return clusters


def fetch_fbo_report(client_id, api_key):
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }

    r = requests.post(
        "https://api-seller.ozon.ru/v1/report/postings/create",
        headers=headers,
        json={
            "filter": {
                "processed_at_from": DATE_FROM,
                "processed_at_to": DATE_TO,
                "delivery_schema": ["fbo"],
                "status": "",
            },
            "language": "DEFAULT",
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"Ошибка создания отчёта FBO: {r.status_code} — {r.text}")
        return []

    code = r.json().get("result", {}).get("code", "")
    print(f"Отчёт FBO создан, код: {code}. Ожидаю...")

    file_url = None
    for attempt in range(20):
        time.sleep(30)
        r2 = requests.post(
            "https://api-seller.ozon.ru/v1/report/info",
            headers=headers, json={"code": code}, timeout=30,
        )
        if r2.status_code != 200:
            continue
        result = r2.json().get("result", {})
        status = result.get("status", "")
        print(f"Попытка {attempt + 1}: {status}")
        if status == "success":
            file_url = result.get("file", "")
            break
        elif status == "failed":
            print(f"Ошибка генерации: {result.get('error', '')}")
            return []

    if not file_url:
        print("Таймаут ожидания отчёта")
        return []

    r3 = requests.get(file_url, timeout=120)
    content = r3.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    return list(reader)


def fetch_fbo(client_id, api_key):
    clusters = fetch_fbo_clusters(client_id, api_key)
    csv_rows = fetch_fbo_report(client_id, api_key)
    rows = []
    for row in csv_rows:
        posting_number = row.get("Номер отправления", "")
        cluster_from, cluster_to = clusters.get(posting_number, ("", ""))
        price = row.get("Ваша цена", "")
        customer_price = row.get("Оплачено покупателем", "")
        rows.append([
            row.get("Номер заказа", ""),
            posting_number,
            row.get("Принят в обработку", ""),
            row.get("Дата отгрузки", ""),
            row.get("Статус", ""),
            row.get("Артикул", ""),
            fmt_num(price),
            row.get("Количество", ""),
            cluster_from,
            cluster_to,
            fmt_num(customer_price),
            calc_spp(price, customer_price),
        ])
    return rows




def main():
    client_id = os.environ["OZON_BM_CLIENT_ID"]
    api_key = os.environ["OZON_BM_API_KEY"]

    print(f"Исторический период: {DATE_FROM[:10]} — {DATE_TO[:10]}")

    fbs_rows = fetch_fbs(client_id, api_key)
    print(f"FBS: {len(fbs_rows)} строк")

    fbo_rows = fetch_fbo(client_id, api_key)
    print(f"FBO: {len(fbo_rows)} строк")

    merge_and_write(SPREADSHEET_ID, FBS_SHEET, FBO_SHEET, fbs_rows, fbo_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
