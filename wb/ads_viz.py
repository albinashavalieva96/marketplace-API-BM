import os
import re
import sys
import requests
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.sheets import write_ads_sheet

SPREADSHEET_ID = "1f5I82g5Nmy3AMn9s0AWta-Hc0HoHSAi9BWlSomzoppM"
SHEET_NAME = "API - WB Виз - Реклама"
DAYS_BACK = 90
CHUNK_DAYS = 30  # WB отдаёт /adv/v1/upd максимум за 1 месяц за раз

ADVERT_TYPE_RU = {
    4: "Кампания в каталоге",
    5: "Кампания в карточке товара",
    6: "Кампания в поиске",
    7: "Кампания в рекомендациях",
    8: "Автоматическая кампания",
    9: "Аукцион",
}

ADVERT_STATUS_RU = {
    -1: "Удалена",
    4: "Готова к запуску",
    7: "Завершена",
    8: "Отклонена",
    9: "Активна",
    11: "На паузе",
}


def fmt_dt(value):
    if not value:
        return ""
    return str(value)[:19].replace("T", " ")


def fmt_num(value, decimals=2):
    try:
        return str(round(float(str(value).replace(",", ".").replace(" ", "")), decimals)).replace(".", ",")
    except (ValueError, TypeError):
        return ""


def date_chunks(start, end, days=CHUNK_DAYS):
    chunks = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks


DATE_SUFFIX_RE = re.compile(r"\s+от\s+\d{1,2}\.\d{1,2}\.\d{2,4}\s*$")


def parse_article(campaign_name):
    """Артикул — часть названия кампании до первого '_', без пробелов по краям
    и без хвоста ' от ДД.ММ.ГГГГ', если он остался (когда в названии нет '_').
    'AKSS01_ПОИСК от 19.03.2026' -> 'AKSS01'
    'AKSS05 _Тюнер Simple ПОЛКИ' -> 'AKSS05'
    'kapo-akss02 от 11.09.2025' -> 'kapo-akss02'"""
    article = campaign_name.split("_", 1)[0].strip()
    article = DATE_SUFFIX_RE.sub("", article).strip()
    return article


def build_row(item):
    advert_id = item.get("advertId", "")
    upd_time = item.get("updTime", "")
    campaign_name = item.get("campName", "")
    key = f"{advert_id}_{upd_time}_{item.get('updNum', 0)}"
    return [
        fmt_dt(upd_time),
        key,
        advert_id,
        campaign_name,
        parse_article(campaign_name),
        ADVERT_TYPE_RU.get(item.get("advertType"), str(item.get("advertType", ""))),
        ADVERT_STATUS_RU.get(item.get("advertStatus"), str(item.get("advertStatus", ""))),
        fmt_num(item.get("updSum", "")),
        item.get("paymentType", ""),
    ]


def fetch_ads(api_key):
    headers = {"Authorization": f"Bearer {api_key}"}
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=DAYS_BACK)

    rows = []
    for chunk_from, chunk_to in date_chunks(start, now):
        params = {
            "from": chunk_from.strftime("%Y-%m-%d"),
            "to": chunk_to.strftime("%Y-%m-%d"),
        }
        r = requests.get(
            "https://advert-api.wildberries.ru/adv/v1/upd",
            headers=headers,
            params=params,
            timeout=60,
        )
        if r.status_code != 200:
            print(f"Ошибка /adv/v1/upd ({params['from']} – {params['to']}): {r.status_code} — {r.text[:200]}")
            continue

        data = r.json() or []
        print(f"  {params['from']} – {params['to']}: {len(data)} записей")
        rows.extend(build_row(item) for item in data)

    return rows


def main():
    api_key = os.environ["WB_VIZ_API_KEY"]

    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Загружаю траты на рекламу WB Виз за последние {DAYS_BACK} дней...")

    rows = fetch_ads(api_key)
    print(f"Всего записей о расходах: {len(rows)}")

    write_ads_sheet(SPREADSHEET_ID, SHEET_NAME, rows)
    print("Готово!")


if __name__ == "__main__":
    main()
