import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}

now = datetime.now(timezone.utc)
date_from_30 = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период (30 дн.): {date_from_30} – {date_to}")
print()

# 1. Затраты за 30 дней — берём пару advertId с реальными списаниями
r = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/upd",
    headers=headers,
    params={"from": date_from_30, "to": date_to},
    timeout=30,
)
print(f"GET /adv/v1/upd → {r.status_code}")
advert_ids = []
if r.status_code == 200:
    data = r.json() or []
    print(f"  Записей: {len(data)}")
    seen = set()
    for item in data:
        aid = item.get("advertId")
        if aid and aid not in seen:
            seen.add(aid)
            advert_ids.append(aid)
    print(f"  Уникальных advertId: {len(advert_ids)} — {advert_ids[:5]}")

print()

if not advert_ids:
    print("Нет advertId для теста — выхожу")
else:
    test_id = advert_ids[0]

    # 2. fullstats v2 — POST с id + interval
    r2 = requests.post(
        "https://advert-api.wildberries.ru/adv/v2/fullstats",
        headers=headers,
        json=[{"id": test_id, "interval": {"begin": date_from_30, "end": date_to}}],
        timeout=30,
    )
    print(f"POST /adv/v2/fullstats (id={test_id}) → {r2.status_code}")
    print(f"  {r2.text[:1500]}")

    print()

    # 3. fullstats v2 — альтернативный формат тела (dates вместо interval)
    r3 = requests.post(
        "https://advert-api.wildberries.ru/adv/v2/fullstats",
        headers=headers,
        json=[{"id": test_id, "dates": [date_from_30, date_to]}],
        timeout=30,
    )
    print(f"POST /adv/v2/fullstats (dates=[{date_from_30},{date_to}]) → {r3.status_code}")
    print(f"  {r3.text[:1500]}")

    print()

    # 4. fullstats v3 — GET с query-параметрами
    r4 = requests.get(
        "https://advert-api.wildberries.ru/adv/v3/fullstats",
        headers=headers,
        params={"ids": test_id, "beginDate": date_from_30, "endDate": date_to},
        timeout=30,
    )
    print(f"GET /adv/v3/fullstats (id={test_id}) → {r4.status_code}")
    print(f"  {r4.text[:1500]}")

    print()

    # 5. Список кампаний с nm/предметами (устаревший в v1, пробуем v0)
    r5 = requests.get(
        "https://advert-api.wildberries.ru/adv/v0/adverts",
        headers=headers,
        timeout=30,
    )
    print(f"GET /adv/v0/adverts → {r5.status_code}")
    print(f"  {r5.text[:500]}")

    print()

    r6 = requests.get(
        f"https://advert-api.wildberries.ru/adv/v1/promotion/adverts",
        headers=headers,
        params={"id": test_id},
        timeout=30,
    )
    print(f"GET /adv/v1/promotion/adverts?id={test_id} → {r6.status_code}")
    print(f"  {r6.text[:500]}")
