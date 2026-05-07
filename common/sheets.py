import json
import os
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

GAP_COLS = 5

DATA_HEADERS = [
    "Номер заказа",
    "Номер отправления",
    "Принят в обработку",
    "Дата отгрузки",
    "Статус",
    "Артикул",
    "Цена продажи",
    "Количество",
    "Кластер отгрузки",
    "Кластер доставки",
    "Оплатили",
    "СПП",
]

# Индексы в строке листа:
# 0 = service cell
# 1..12 = FBS data (12 столбцов)
# 13..17 = gap (5 столбцов)
# 18 = FBO service cell
# 19..30 = FBO data (12 столбцов)
FBS_START = 1
FBS_END = FBS_START + len(DATA_HEADERS)          # 13
FBO_START = FBS_END + GAP_COLS + 1               # 19
FBO_END = FBO_START + len(DATA_HEADERS)           # 31

# Индекс posting_number внутри data-строки (до добавления service cell)
IDX_POSTING = 1   # row[1] = Номер отправления
IDX_DATE    = 2   # row[2] = Принят в обработку


def get_sheets_client():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def merge_and_write(spreadsheet_id, sheet_name, fbs_new, fbo_new):
    """Читает существующие данные листа, добавляет новые, сортирует по дате и записывает."""
    client = get_sheets_client()
    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=20000, cols=40)

    # Читаем существующие данные
    existing = worksheet.get_all_values()
    fbs_dict = {}
    fbo_dict = {}

    for row in existing[1:]:  # пропускаем заголовок
        row = list(row) + [""] * 40  # дополняем пустыми ячейками

        fbs_posting = row[FBS_START + IDX_POSTING]  # колонка C
        if fbs_posting:
            fbs_dict[fbs_posting] = row[FBS_START:FBS_END]

        fbo_posting = row[FBO_START + IDX_POSTING]  # колонка U
        if fbo_posting:
            fbo_dict[fbo_posting] = row[FBO_START:FBO_END]

    # Добавляем/обновляем новыми данными
    for row in fbs_new:
        fbs_dict[row[IDX_POSTING]] = row

    for row in fbo_new:
        fbo_dict[row[IDX_POSTING]] = row

    # Сортируем по дате убыванию
    def sort_key(r):
        return r[IDX_DATE] if len(r) > IDX_DATE else ""

    fbs_sorted = sorted(fbs_dict.values(), key=sort_key, reverse=True)
    fbo_sorted = sorted(fbo_dict.values(), key=sort_key, reverse=True)

    # Формируем строки листа
    gap = [""] * GAP_COLS
    header_row = ["Заказы FBS"] + DATA_HEADERS + gap + ["Заказы FBO"] + DATA_HEADERS

    max_rows = max(len(fbs_sorted), len(fbo_sorted), 1)
    empty = [""] * len(DATA_HEADERS)
    fbs_padded = fbs_sorted + [empty] * (max_rows - len(fbs_sorted))
    fbo_padded = fbo_sorted + [empty] * (max_rows - len(fbo_sorted))

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    service = ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")]

    all_rows = [header_row]
    for i, (fbs, fbo) in enumerate(zip(fbs_padded, fbo_padded)):
        service_cell = service[i] if i < len(service) else ""
        all_rows.append([service_cell] + list(fbs) + gap + [""] + list(fbo))

    worksheet.clear()
    worksheet.update("A1", all_rows)

    print(f"Итого FBS: {len(fbs_sorted)}, FBO: {len(fbo_sorted)} → лист '{sheet_name}'")
