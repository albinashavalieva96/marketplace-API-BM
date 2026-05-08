import os
import sys
import requests
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - ЯМ Виз - Заказы"

CAMPAIGN_IDS = [22110675, 56291750]

# Период для исторической загрузки (DD-MM-YYYY)
DATE_FROM = "01-06-2025"
DATE_TO   = "01-09-2025"

STATUS_MAP = {
    "CANCELLED": "Отменено",
    "DELIVERED": "Доставлено",
    "DELIVERY": "Доставляется",
    "PICKUP": "Пункт выдачи",
    "PROCESSING": "В обработке",
    "PENDING": "Ожидает подтверждения",
    "UNPAID": "Ожидает оплаты",
    "CANCELLED_IN_DELIVERY": "Отменен при доставке",
}


def fmt_ym_dt(value):
    if not value:
        return ""
    try:
        parts = str(value).split(" ")
        d, m, y = parts[0].split("-")
        result = f"{y}-{m}-{d}"
        if len(parts) > 1:
            result += f" {parts[1]}"
        return result
    except Exception:
        return str(value)


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def calc_spp(price, buyer_price):
    try:
        p = float(str(price).replace(",", "."))
        bp = float(str(buyer_price).replace(",", "."))
        return str(round((p - bp) / p, 10)).replace(".", ",") if bp and p else ""
    except (ValueError, TypeError):
        return ""


def fetch_campaign_orders(api_token, campaign_id):
    headers = {"Authorization": f"Bearer {api_token}"}
    rows = []
    page = 1

    while True:
        r = requests.get(
            f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/orders",
            headers=headers,
            params={"fromDate": DATE_FROM, "toDate": DATE_TO, "limit": 50, "page": page},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"Ошибка ЯМ кампания {campaign_id}: {r.status_code} — {r.text[:300]}")
            break

        data = r.json()
        result = data.get("result", data)
        orders = result.get("orders", [])
        pager = result.get("pager", {})

        for order in orders:
            status = STATUS_MAP.get(order.get("status", ""), order.get("status", ""))
            delivery = order.get("delivery", {})
            shipment = delivery.get("shipment", {})
            region = delivery.get("region", {})
            partner_type = delivery.get("deliveryPartnerType", "")
            supply_type = "FBY" if partner_type == "YANDEX_MARKET" else "FBS"
            warehouse_name = shipment.get("warehouseName", "")
            region_name = region.get("name", "")
            shipment_date = shipment.get("date", "")

            for item in order.get("items", []):
                price = item.get("price", "")
                buyer_price = item.get("buyerPrice", "")
                posting = f"{order.get('id', '')}_{item.get('id', '')}"
                rows.append([
                    str(order.get("id", "")),
                    posting,
                    fmt_ym_dt(order.get("creationDate", "")),
                    fmt_ym_dt(shipment_date),
                    status,
                    item.get("offerId", ""),
                    fmt_num(price),
                    item.get("count", 0),
                    warehouse_name,
                    region_name,
                    fmt_num(buyer_price),
                    calc_spp(price, buyer_price),
                    supply_type,
                ])

        pages_count = pager.get("pagesCount", 1)
        if page >= pages_count:
            break
        page += 1

    return rows


def main():
    api_token = os.environ["YM_VIZ_API_TOKEN"]

    print(f"Исторический период: {DATE_FROM} — {DATE_TO}")

    all_rows = []
    for campaign_id in CAMPAIGN_IDS:
        rows = fetch_campaign_orders(api_token, campaign_id)
        print(f"Кампания {campaign_id}: {len(rows)} строк")
        all_rows.extend(rows)

    print(f"Итого: {len(all_rows)} строк")
    write_sheet(SPREADSHEET_ID, SHEET_NAME, all_rows)
    print("Готово!")


if __name__ == "__main__":
    main()
