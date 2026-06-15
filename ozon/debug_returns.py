import os
import json
import requests

client_id = os.environ["OZON_BM_CLIENT_ID"]
api_key = os.environ["OZON_BM_API_KEY"]

headers = {
    "Client-Id": client_id,
    "Api-Key": api_key,
    "Content-Type": "application/json",
}

print("=== /v1/returns/list — первые 5 возвратов (полная структура) ===")
r = requests.post(
    "https://api-seller.ozon.ru/v1/returns/list",
    headers=headers,
    json={"limit": 5, "offset": 0},
    timeout=30,
)
print(f"HTTP: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    returns = data.get("returns", [])
    print(f"Возвратов в ответе: {len(returns)}")
    if returns:
        print("\n--- Первый возврат (все поля) ---")
        for k, v in returns[0].items():
            print(f"  {k}: {v}")
        if len(returns) > 1:
            print("\n--- Второй возврат (все поля) ---")
            for k, v in returns[1].items():
                print(f"  {k}: {v}")
    # Пробуем пагинацию — есть ли поле с total?
    print(f"\nКлючи верхнего уровня ответа: {list(data.keys())}")
    for k, v in data.items():
        if k != "returns":
            print(f"  {k}: {v}")
else:
    print(r.text[:500])
