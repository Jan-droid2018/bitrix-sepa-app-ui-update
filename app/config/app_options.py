import json

from app.services.bitrix_helper import b24_call

# =========================
# App-Optionen (pro Portal)
# =========================

APP_OPT_PREFIX = "SEPA_SDD_"


def _normalize_app_option(name: str, value) -> str | None:
    option_key = APP_OPT_PREFIX + name

    if isinstance(value, dict):
        if option_key in value:
            value = value.get(option_key)
        elif value.get("option") == option_key and "value" in value:
            # Recovery path for the old broken payload shape that stored
            # literal "option"/"value" keys instead of the actual option name.
            value = value.get("value")
        elif set(value.keys()) == {"value"}:
            value = value.get("value")
        else:
            return None

    if value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    return value or None


def app_opt_get(domain: str, token: str, name: str) -> str | None:
    try:
        val = b24_call(domain, token, "app.option.get", {"option": APP_OPT_PREFIX + name})
        return _normalize_app_option(name, val)
    except Exception:
        return None


def app_opt_set(domain: str, token: str, name: str, value: str):
    b24_call(
        domain,
        token,
        "app.option.set",
        {"options": {APP_OPT_PREFIX + name: value if value is not None else ""}},
    )


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
