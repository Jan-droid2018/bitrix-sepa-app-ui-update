import time

import requests

from app.config.app_options import app_opt_get, app_opt_set
from app.config.oauth import get_oauth_credentials

BITRIX_OAUTH_URL = "https://oauth.bitrix.info/oauth/token/"


def refresh_access_token(domain: str, refresh_token: str) -> dict:
    del domain
    if not refresh_token:
        raise RuntimeError("Kein Refresh-Token vorhanden.")

    client_id, client_secret = get_oauth_credentials()
    resp = requests.post(
        BITRIX_OAUTH_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=20,
    )

    try:
        data = resp.json()
    except Exception:
        data = {}

    if resp.status_code >= 400 or "access_token" not in data:
        msg = data.get("error_description") or data.get("error") or resp.text[:200]
        raise RuntimeError(f"Token refresh failed: {msg}")

    return data


def expires_at_from_now(expires_in) -> int:
    return int(time.time()) + max(0, int(expires_in or 0) - 60)


def get_valid_access_token(domain: str, bootstrap_token: str) -> str:
    """
    bootstrap_token = aktuelles Token aus Request ODER letztes bekanntes Token.
    Fallback-sicher: ohne gespeicherte Refresh-Daten wird das Bootstrap-Token
    unveraendert zurueckgegeben, statt rekursiv zu scheitern.
    """

    access = app_opt_get(domain, bootstrap_token, "ACCESS_TOKEN") or bootstrap_token
    refresh = app_opt_get(domain, bootstrap_token, "REFRESH_TOKEN")

    try:
        expires_at = int(app_opt_get(domain, bootstrap_token, "EXPIRES_AT") or 0)
    except Exception:
        expires_at = 0

    if access and time.time() < expires_at:
        return access

    if not refresh:
        return access or bootstrap_token

    data = refresh_access_token(domain, refresh)
    expires_at = expires_at_from_now(data.get("expires_in"))

    app_opt_set(domain, data["access_token"], "ACCESS_TOKEN", data["access_token"])
    app_opt_set(domain, data["access_token"], "REFRESH_TOKEN", data["refresh_token"])
    app_opt_set(domain, data["access_token"], "EXPIRES_AT", str(expires_at))

    return data["access_token"]
