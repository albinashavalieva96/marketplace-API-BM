import os
import json
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}

now = datetime.now(timezone.utc)
date_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период: {date_from} – {date_to}")
print()

# 1. Список рекламных кампаний (группы по типу/статусу + ID)
r = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/promotion/count",
    headers=headers,
    timeout=30,
)
print(f"GET /adv/v1/promotion/count → {r.status_code}")
advert_ids = []
if r.status_code == 200:
    data = r.json()
    for group in data.get("adverts", []):
        for item in group.get("advert_list", []):
            aid = item.get("advertId")
            if aid:
                advert_ids.append(aid)
    print(f"  Всего кампаний: {len(advert_ids)}")
    print(f"  {r.text[:300]}")

print()

# 2. Детали кампаний — POST с ID (GET устарел, отдаёт 404)
if advert_ids:
    r2 = requests.post(
        "https://advert-api.wildberries.ru/adv/v1/promotion/adverts",
        headers=headers,
        json=advert_ids[:50],
        timeout=30,
    )
    print(f"POST /adv/v1/promotion/adverts (первые 50 ID) → {r2.status_code}")
    if r2.status_code == 200:
        adv_data = r2.json() or []
        print(f"  Кампаний: {len(adv_data)}")
        if adv_data:
            print("  Первая кампания:")
            print(json.dumps(adv_data[0], ensure_ascii=False, indent=2)[:800])
    else:
        print(f"  {r2.text[:300]}")

print()

# 3. Расходы по кампаниям — интервал максимум 1 месяц
upd_from = (now - timedelta(days=30)).strftime("%Y-%m-%d")
r3 = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/upd",
    headers=headers,
    params={"from": upd_from, "to": date_to},
    timeout=30,
)
print(f"GET /adv/v1/upd (расходы, {upd_from} – {date_to}) → {r3.status_code}")
if r3.status_code == 200:
    data = r3.json() or []
    print(f"  Записей: {len(data)}")
    if data:
        print("  Первая запись:")
        print(json.dumps(data[0], ensure_ascii=False, indent=2)[:800])
        print("  Вторая запись:" if len(data) > 1 else "")
        if len(data) > 1:
            print(json.dumps(data[1], ensure_ascii=False, indent=2)[:800])
else:
    print(f"  {r3.text[:300]}")
