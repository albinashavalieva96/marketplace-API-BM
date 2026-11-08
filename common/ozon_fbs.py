"""Список отправлений FBS Ozon через /v4/posting/fbs/list.

/v3/posting/fbs/list Ozon отключил (404). В v4 страницы идут по cursor, а скрипты написаны под offset,
поэтому v4_fbs_post принимает прежний запрос (offset, limit), один раз забирает все страницы и отдает нужный срез.
"""
import json as _json
import requests

_cache = {}


class _Resp:
    def __init__(self, status_code, text, payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        return self._payload


def _shape(x):
    if isinstance(x, dict):
        return {k: _shape(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_shape(x[0])] if x else []
    return type(x).__name__


def _load(url, headers, body, timeout):
    postings, seen, cursor = [], set(), ""
    for _ in range(500):
        req = dict(body)
        if cursor:
            req["cursor"] = cursor
        r = requests.post(url, headers=headers, json=req, timeout=timeout)
        if r.status_code != 200:
            print(f"Ошибка Ozon FBS v4: {r.status_code} {r.text[:300]}")
            return None, r
        data = r.json()
        result = data.get("result", data)
        page = result if isinstance(result, list) else result.get("postings", [])
        if page and not postings:
            print("СТРУКТУРА v4:", _json.dumps({k: _shape(page[0].get(k)) for k in ("products", "financial_data")}, ensure_ascii=False))
        for p in page:
            number = p.get("posting_number", "")
            if number not in seen:
                seen.add(number)
                postings.append(p)
        meta = result if isinstance(result, dict) else data
        cursor = meta.get("cursor") or data.get("cursor") or ""
        if not page or not cursor or meta.get("has_next", data.get("has_next")) is False:
            break
    return postings, None


def v4_fbs_post(url, headers=None, json=None, timeout=30):
    """Замена requests.post для списка FBS: понимает limit и offset из старого запроса."""
    src = dict(json or {})
    offset = int(src.pop("offset", 0) or 0)
    limit = int(src.pop("limit", 100) or 100)
    src.pop("dir", None)
    flt = {k: v for k, v in (src.get("filter") or {}).items() if v not in ("", None)}
    body = {"filter": flt, "limit": 100}
    if src.get("with"):
        body["with"] = src["with"]
    key = _json.dumps([headers.get("Client-Id"), body], sort_keys=True, ensure_ascii=False)
    if key not in _cache:
        postings, err = _load(url, headers, body, timeout)
        if err is not None:
            return err
        _cache[key] = postings
    part = _cache[key][offset:offset + limit]
    return _Resp(200, "", {"result": {"postings": part}})
