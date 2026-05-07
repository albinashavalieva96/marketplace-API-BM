import json
import os
import gspread
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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

IDX_POSTING = 1  # row[1] = Номер отправления
IDX_DATE    = 2  # row[2] = Принят в обработку

# В листе: col A = service cell, cols B.. = данные
SHEET_DATA_START = 1


def get_sheets_client():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _write_sheet(spreadsheet, sheet_name, new_rows):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, rows=1000, cols=len(DATA_HEADERS) + 1
        )

    existing = worksheet.get_all_values()
    data_dict = {}

    for row in existing[1:]:  # пропускаем заголовок
        row = list(row) + [""] * 20
        posting = row[SHEET_DATA_START + IDX_POSTING]
        if posting:
            data_dict[posting] = row[SHEET_DATA_START:SHEET_DATA_START + len(DATA_HEADERS)]

    for row in new_rows:
        data_dict[row[IDX_POSTING]] = row

    sorted_rows = sorted(
        data_dict.values(),
        key=lambda r: r[IDX_DATE] if len(r) > IDX_DATE else "",
    )

    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    service = ["Обновлен:", now.strftime("%Y-%m-%d"), now.strftime("%H:%M")]

    header_row = [""] + DATA_HEADERS
    all_rows = [header_row]
    for i, data_row in enumerate(sorted_rows):
        service_cell = service[i] if i < len(service) else ""
        all_rows.append([service_cell] + list(data_row))

    worksheet.resize(rows=max(len(all_rows), 1), cols=len(DATA_HEADERS) + 1)
    worksheet.update("A1", all_rows)

    return len(sorted_rows)


def merge_and_write(spreadsheet_id, fbs_sheet_name, fbo_sheet_name, fbs_new, fbo_new):
    client = get_sheets_client()
    spreadsheet = client.open_by_key(spreadsheet_id)

    fbs_count = _write_sheet(spreadsheet, fbs_sheet_name, fbs_new)
    fbo_count = _write_sheet(spreadsheet, fbo_sheet_name, fbo_new)

    print(f"FBS: {fbs_count} → '{fbs_sheet_name}', FBO: {fbo_count} → '{fbo_sheet_name}'")
