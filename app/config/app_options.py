import json

from app.services.bitrix_helper import b24_call

# =========================
# App-Optionen (pro Portal)
# =========================

APP_OPT_PREFIX = "SEPA_SDD_"


def app_opt_get(domain: str, token: str, name: str) -> str | None:
    try:
        val = b24_call(domain, token, "app.option.get", {"option": APP_OPT_PREFIX + name}) or None
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return None
        return val
    except Exception:
        return None


def app_opt_set(domain: str, token: str, name: str, value: str):
    b24_call(domain, token, "app.option.set", {"option": APP_OPT_PREFIX + name, "value": value})


def load_fieldmap(domain: str, token: str) -> dict:
    raw = app_opt_get(domain, token, "FIELDMAP_JSON")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def save_fieldmap(domain: str, token: str, fmap: dict):
    app_opt_set(domain, token, "FIELDMAP_JSON", json.dumps(fmap))
