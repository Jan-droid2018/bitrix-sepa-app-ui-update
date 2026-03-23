import random
import time

import requests
from flask import has_request_context, session

_session = requests.Session()
# =========================
# Bitrix REST – Helper
# =========================

def _flatten_params(prefix, value, out_list):
    if isinstance(value, dict):
        for k, v in value.items():
            new_prefix = f"{prefix}[{k}]" if prefix else k
            _flatten_params(new_prefix, v, out_list)
    elif isinstance(value, (list, tuple)):
        for v in value:
            new_prefix = f"{prefix}[]" if prefix else "[]"
            _flatten_params(new_prefix, v, out_list)
    else:
        out_list.append((prefix, value))


def _extract_error_message(data, text: str) -> str:
    if isinstance(data, dict):
        return str(data.get("error_description") or data.get("error") or text[:200])
    return text[:200]


def _is_expired_token_message(message: str) -> bool:
    normalized = (message or "").lower()
    return any(
        marker in normalized
        for marker in (
            "access token provided has expired",
            "expired token",
            "expired_token",
            "invalid token",
            "token expired",
        )
    )


def _refresh_request_token(domain: str, current_token: str | None) -> str | None:
    if not has_request_context():
        return None

    from app.services.token_manager import expires_at_from_now, refresh_access_token

    refresh_token = session.get("refresh_token")
    if not refresh_token:
        return None

    session_domain = session.get("domain")
    if session_domain and session_domain != domain:
        return None

    session_token = session.get("token")
    if session_token and current_token and session_token != current_token:
        return session_token

    try:
        refreshed = refresh_access_token(domain, refresh_token)
    except Exception:
        return None

    new_token = refreshed.get("access_token")
    if not new_token:
        return None

    session["token"] = new_token
    if refreshed.get("refresh_token"):
        session["refresh_token"] = refreshed["refresh_token"]
    session["expires_at"] = expires_at_from_now(refreshed.get("expires_in"))
    return new_token


def _post(domain: str, method: str, params: dict, retries: int = 5):
    url = f"https://{domain}/rest/{method}"
    attempt = 0
    token_refresh_attempted = False
    while True:
        form_items: list[tuple[str, str]] = []
        for k, v in params.items():
            _flatten_params(k, v, form_items)

        # Request senden
        r = _session.post(url, data=form_items, timeout=20)

        # Antwort aufbereiten (damit text/data IMMER gesetzt sind)
        text = r.text
        try:
            data = r.json()
        except Exception:
            data = {}

        error_message = _extract_error_message(data, text)

        if _is_expired_token_message(error_message) and not token_refresh_attempted:
            fresh_token = _refresh_request_token(domain, params.get("auth"))
            if fresh_token and fresh_token != params.get("auth"):
                params = params.copy()
                params["auth"] = fresh_token
                token_refresh_attempted = True
                continue

        # Rate-Limit (HTTP 429 ODER Bitrix-Fehler im JSON)
        if r.status_code == 429 or (
                isinstance(data, dict) and data.get("error") in ("TOO_MANY_REQUESTS", "QUERY_LIMIT_EXCEEDED")):
            attempt += 1
            if attempt >= retries:
                raise RuntimeError(f"REST {method} fehlgeschlagen: zu viele Anfragen (429) nach {retries} Versuchen")

            # Retry-After respektieren, sonst Exponential Backoff + Jitter
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except Exception:
                    time.sleep(1.0)
            else:
                delay = min(0.5 * (2 ** attempt), 10.0) * (0.75 + 0.5 * random.random())
                time.sleep(delay)
            continue

        # Andere HTTP-Fehler
        if r.status_code >= 400:
            raise RuntimeError(f"REST {method} fehlgeschlagen: {error_message}")

        # Bitrix-Fehler trotz 200
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Bitrix REST Error: {error_message}")

        # Erfolg
        return data


def b24_call(domain, access_token, method, params=None, retry=True):
    del retry
    data = _post(domain, method, (params or {}) | {"auth": access_token})
    return data.get("result")

def b24_call_raw(domain: str, access_token: str, method: str, params: dict | None = None):
    return _post(domain, method, (params or {}) | {"auth": access_token})


def b24_list_all(domain: str, access_token: str, method: str, params: dict | None = None, max_items: int | None = None):
    out = []
    start = 0
    seen_starts = set()
    while True:
        if start in seen_starts:
            break
        seen_starts.add(start)

        payload = (params or {}).copy()
        payload["start"] = start
        data = b24_call_raw(domain, access_token, method, payload)
        chunk = data.get("result", [])
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk["items"]
        out.extend(chunk)
        if max_items is not None and len(out) >= max_items:
            out = out[:max_items]
            break
        nxt = data.get("next")
        if nxt is None:
            break
        start = nxt
    return out


# =========================
# Batch Helper (bis 50 pro Batch)
# =========================

def _chunk(seq, n):
    it = iter(seq)
    while True:
        buf = list()
        try:
            for _ in range(n):
                buf.append(next(it))
        except StopIteration:
            if buf:
                yield buf
            break
        yield buf


def b24_batch(domain: str, access_token: str, commands: dict, halt: bool = False) -> dict:
    """
    commands: {"key": "method?param1=...&param2=..."}
    Rückgabe: {"key": <result>, ...}
    """
    res = {}
    for part in _chunk(list(commands.items()), 50):
        cmd = {}
        for k, v in part:
            cmd[k] = v
        payload = {"halt": 1 if halt else 0, "cmd": cmd, "auth": access_token}
        data = _post(domain, "batch", payload)
        inner = (data.get("result") or {}).get("result") or {}
        res.update(inner)
    return res


def get_auth_from_request(req) -> dict:
    auth = {
        "access_token": req.values.get("auth[access_token]") or req.values.get("AUTH_ID"),
        "domain": req.values.get("auth[domain]") or req.values.get("DOMAIN"),
        "member_id": req.values.get("auth[member_id]") or req.values.get("member_id"),
        "user_id": req.values.get("auth[user_id]") or req.values.get("USER_ID"),
        "refresh_token": req.values.get("auth[refresh_token]") or req.values.get("REFRESH_ID"),
        "expires": req.values.get("auth[expires]") or req.values.get("expires"),
    }
    if not auth["access_token"] or not auth["domain"]:
        raise RuntimeError("Fehlende auth-Parameter: access_token/domain")
    return auth
