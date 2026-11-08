"""Деньги в пути к клиенту и от клиента по данным API: снимок на момент запуска.

ВБ: отчет об остатках на складах (строки «в пути до получателей» и «в пути возвраты»), сумма = штуки × средняя цена заказа.
Озон и ЯМ: активные возвраты. Лист перезаписывается целиком; кабинет, по которому API не ответил, остается как был,
а задание завершается ошибкой, чтобы сбой был виден.
"""
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import get_sheets_client

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET = "API - В пути (клиент)"
HEADER = ["Кабинет", "Направление", "Артикул", "Кол-во", "Цена", "Сумма", "Источник"]
TO_CLIENT, FROM_CLIENT = "К клиенту", "От клиента"
MSK = timezone(timedelta(hours=3))
OZON_DONE = {"ReceivedBySeller", "Cancelled", "CancelledBySeller"}
YM_CAMPAIGNS = {"ЯМ Виз": ("YM_VIZ_API_TOKEN", [22110675, 56291750]), "ЯМ Бар": ("YM_BAR_API_TOKEN", [147572980])}


def num(value):
    try:
        return float(str(value).replace("\xa0", "").replace(" ", "").replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def shape(x):
    """Только имена и типы полей: журналы репозитория открытые."""
    if isinstance(x, dict):
        return {k: shape(v) for k, v in x.items()}
    if isinstance(x, list):
        return [shape(x[0])] if x else []
    return type(x).__name__


def wb_prices(spreadsheet, cabinet):
    """Средняя цена оплаты клиента по артикулу (заказы за 60 дней, если их нет — за все время)."""
    values = spreadsheet.worksheet(f"API - WB {cabinet} - Заказы").get_all_values()
    since = (datetime.now(MSK) - timedelta(days=60)).strftime("%Y-%m-%d")
    recent, every = defaultdict(list), defaultdict(list)
    for row in values[1:]:
        row = list(row) + [""] * 16
        article, price = row[6], num(row[11])
        if not article or price <= 0 or row[5] == "Отменён":
            continue
        every[article].append(price)
        if row[3][:10] >= since:
            recent[article].append(price)
    return {a: sum(recent.get(a) or p) / len(recent.get(a) or p) for a, p in every.items()}


def wb_rows(api_key, cabinet, prices):
    base = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
    head = {"Authorization": f"Bearer {api_key}"}
    r = requests.get(base, headers=head, params={"locale": "ru", "groupBySa": "true", "groupByNm": "true"}, timeout=60)
    r.raise_for_status()
    task = r.json()["data"]["taskId"]
    for _ in range(60):
        time.sleep(10)
        s = requests.get(f"{base}/tasks/{task}/status", headers=head, timeout=60)
        s.raise_for_status()
        status = s.json()["data"]["status"]
        if status == "done":
            break
        if status in ("purged", "canceled"):
            raise RuntimeError(f"отчет ВБ: статус {status}")
    else:
        raise RuntimeError("отчет ВБ не готов за 10 минут")
    d = requests.get(f"{base}/tasks/{task}/download", headers=head, timeout=120)
    d.raise_for_status()
    data = d.json()
    if data:
        print(cabinet, "поля отчета:", shape(data[0]))
    names = {w.get("warehouseName", "") for x in data for w in x.get("warehouses", [])}
    print(cabinet, "строки складов:", sorted(n for n in names if re.search("пути|всего", n, re.I)))
    rows = []
    for item in data:
        article = item.get("vendorCode", "")
        for w in item.get("warehouses", []):
            name, qty = w.get("warehouseName", ""), num(w.get("quantity"))
            if qty <= 0 or not re.search("в пути", name, re.I):
                continue
            direction = FROM_CLIENT if re.search("возврат", name, re.I) else TO_CLIENT
            price = prices.get(article, 0)
            rows.append([f"WB {cabinet}", direction, article, qty, round(price, 2), round(qty * price, 2), name])
    return rows


def ozon_rows(client_id, api_key, cabinet):
    head = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows, last_id = [], 0
    for _ in range(100):
        body = {"limit": 500, "last_id": last_id} if last_id else {"limit": 500, "offset": 0}
        r = requests.post("https://api-seller.ozon.ru/v1/returns/list", headers=head, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        returns = data.get("returns", [])
        for ret in returns:
            product = ret.get("product") or {}
            status = (ret.get("visual") or {}).get("status") or {}
            if status.get("sys_name", "") in OZON_DONE:
                continue
            qty = num(product.get("quantity")) or 1
            price = num((product.get("price") or {}).get("price"))
            article = product.get("offer_id", "") or str(product.get("sku", ""))
            rows.append([cabinet, FROM_CLIENT, article, qty, price, round(qty * price, 2), status.get("display_name", "")])
        if not data.get("has_next") or not returns:
            break
        last_id = returns[-1].get("id", 0)
        if not last_id:
            break
    return rows


def ym_order_prices(spreadsheet, cabinet):
    values = spreadsheet.worksheet(f"API - {cabinet} - Заказы").get_all_values()
    prices = {}
    for row in values[1:]:
        row = list(row) + [""] * 16
        prices[(row[1], row[6])] = num(row[11])
    return prices


def ym_rows(token, campaigns, cabinet, prices):
    head = {"Api-Key": token}
    since = (datetime.now(MSK) - timedelta(days=90)).strftime("%Y-%m-%d")
    rows, counts, shown = [], defaultdict(int), False
    done_ship = ("RECEIVED", "RECEIVED_ON_FULFILLMENT", "DELIVERED", "UTILIZED", "READY_FOR_PICKUP", "CANCELLED")
    done_refund = ("REFUND_REJECTED", "CANCELLED", "DECISION_MADE_REJECT")
    for campaign in campaigns:
        page_token = None
        for _ in range(200):
            params = {"limit": 50, "fromDate": since}
            if page_token:
                params["pageToken"] = page_token
            r = requests.get(f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign}/returns",
                             headers=head, params=params, timeout=60)
            r.raise_for_status()
            result = r.json().get("result", {})
            returns = result.get("returns", [])
            if returns and not shown:
                print(cabinet, "поля возврата:", shape(returns[0]))
                shown = True
            for ret in returns:
                ship, refund = ret.get("shipmentStatus", ""), ret.get("refundStatus", "")
                counts[f"{ship}/{refund}"] += 1
                if ship in done_ship or refund in done_refund:
                    continue
                for item in ret.get("items", []):
                    article, qty = item.get("shopSku", ""), num(item.get("count")) or 1
                    price = prices.get((str(ret.get("orderId", "")), article), 0)
                    rows.append([cabinet, FROM_CLIENT, article, qty, price, round(qty * price, 2), f"{ship}/{refund}"])
            page_token = (result.get("paging") or {}).get("nextPageToken")
            if not page_token:
                break
    print(cabinet, "статусы возвратов:", dict(counts))
    return rows


def main():
    spreadsheet = get_sheets_client().open_by_key(SPREADSHEET_ID)
    jobs = {}
    for cabinet, env in (("Виз", "WB_VIZ_API_KEY"), ("Бар", "WB_BAR_API_KEY")):
        jobs[f"WB {cabinet}"] = lambda c=cabinet, e=env: wb_rows(os.environ[e], c, wb_prices(spreadsheet, c))
    jobs["Озон BM"] = lambda: ozon_rows(os.environ["OZON_BM_CLIENT_ID"], os.environ["OZON_BM_API_KEY"], "Озон BM")
    jobs["Озон CF"] = lambda: ozon_rows(os.environ["OZON_CF_CLIENT_ID"], os.environ["OZON_CF_API_KEY"], "Озон CF")
    for cabinet, (env, campaigns) in YM_CAMPAIGNS.items():
        jobs[cabinet] = lambda c=cabinet, e=env, m=campaigns: ym_rows(os.environ[e], m, c, ym_order_prices(spreadsheet, c))

    try:
        sheet = spreadsheet.worksheet(SHEET)
    except Exception:
        sheet = spreadsheet.add_worksheet(title=SHEET, rows=1000, cols=len(HEADER))
    old = sheet.get_all_values()[2:]
    rows, failed = [], []
    for cabinet, job in jobs.items():
        try:
            part = job()
            print(f"{cabinet}: {len(part)} строк")
            rows += part
        except Exception as e:
            print(f"Ошибка {cabinet}: {str(e)[:300]}")
            failed.append(cabinet)
            rows += [r for r in old if r and r[0] == cabinet]

    stamp = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    table = [[f"Обновлен: {stamp}"], HEADER] + rows
    sheet.resize(rows=max(len(table), 3), cols=len(HEADER))
    sheet.clear()
    sheet.update("A1", table)
    print(f"Итого строк: {len(rows)}")
    if failed:
        sys.exit("Не обновились: " + ", ".join(failed))


if __name__ == "__main__":
    main()
