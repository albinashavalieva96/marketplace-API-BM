import os
import requests
from datetime import datetime, timedelta, timezone

api_token = os.environ["YM_VIZ_API_TOKEN"]
headers = {"Api-Key": api_token}

now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=90)).strftime("%d-%m-%Y")
date_to = now.strftime("%d-%m-%Y")

# ЯМ Виз кампании
campaign_ids = [22110675, 56291750]

print(f"Период: {date_from} – {date_to}")
print()

# 1. Попробуем финансовый отчёт через stats/orders с финансовыми данными
for campaign_id in campaign_ids:
    print(f"=== Кампания {campaign_id} ===")

    # Статистика заказов с финансами
    r = requests.post(
        f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders",
        headers=headers,
        json={
            "dateFrom": date_from,
            "dateTo": date_to,
        },
        timeout=30,
    )
    print(f"POST /stats/orders → {r.status_code}")
    if r.status_code == 200:
        data = r.json().get("result", {})
        print(f"  Поля: {list(data.keys())[:10]}")

    # Финансовые транзакции
    r2 = requests.get(
        f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/billing/accounts",
        headers=headers,
        timeout=30,
    )
    print(f"GET /billing/accounts → {r2.status_code}")
    if r2.status_code == 200:
        print(f"  Ответ: {r2.text[:300]}")

    # Расходы на продвижение
    r3 = requests.get(
        f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/auction/recommendations",
        headers=headers,
        timeout=30,
    )
    print(f"GET /auction/recommendations → {r3.status_code}")

    # Отчёт по рекламе
    r4 = requests.post(
        f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/main-boost",
        headers=headers,
        json={"dateFrom": date_from, "dateTo": date_to},
        timeout=30,
    )
    print(f"POST /stats/main-boost → {r4.status_code}")
    if r4.status_code == 200:
        print(f"  Ответ: {r4.text[:300]}")

    print()
