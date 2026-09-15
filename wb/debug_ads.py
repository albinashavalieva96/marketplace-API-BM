import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

api_key = os.environ["WB_VIZ_API_KEY"]
headers = {"Authorization": f"Bearer {api_key}"}

now = datetime.now(timezone.utc)
date_from_30 = (now - timedelta(days=30)).strftime("%Y-%m-%d")
date_to = now.strftime("%Y-%m-%d")

print(f"Период (30 дн.): {date_from_30} – {date_to}")
print()

r = requests.get(
    "https://advert-api.wildberries.ru/adv/v1/upd",
    headers=headers,
    params={"from": date_from_30, "to": date_to},
    timeout=30,
)
advert_ids = []
if r.status_code == 200:
    data = r.json() or []
    seen = set()
    for item in data:
        aid = item.get("advertId")
        if aid and aid not in seen:
            seen.add(aid)
            advert_ids.append(aid)

print(f"Уникальных advertId: {len(advert_ids)} — {advert_ids[:6]}")
print()

for i, test_id in enumerate(advert_ids[:3]):
    if i > 0:
        time.sleep(20)
    r4 = requests.get(
        "https://advert-api.wildberries.ru/adv/v3/fullstats",
        headers=headers,
        params={"ids": test_id, "beginDate": date_from_30, "endDate": date_to},
        timeout=30,
    )
    print(f"=== advertId={test_id} → {r4.status_code} ===")
    if r4.status_code != 200:
        print(f"  {r4.text[:300]}")
        print()
        continue

    payload = r4.json()
    if not payload:
        print("  Пустой ответ")
        print()
        continue

    camp = payload[0]
    print(f"  Верхний уровень, ключи: {list(camp.keys())}")

    days = camp.get("days", [])
    print(f"  Дней в ответе: {len(days)}")
    if days:
        day0 = days[-1]  # последний день — самый свежий
        print(f"  Ключи дня: {list(day0.keys())}")
        print(f"  День: date={day0.get('date')} sum={day0.get('sum')} views={day0.get('views')} clicks={day0.get('clicks')}")
        apps = day0.get("apps", [])
        print(f"  apps в этом дне: {len(apps)}")
        for app in apps:
            print(f"    appType={app.get('appType')} sum={app.get('sum')} ключи={list(app.keys())}")
            nms = app.get("nms", [])
            print(f"    nms-записей: {len(nms)}")
            for nm in nms[:5]:
                print(f"      {json.dumps(nm, ensure_ascii=False)}")
    print()
