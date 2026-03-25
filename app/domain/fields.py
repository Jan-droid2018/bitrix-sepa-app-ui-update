import os
from functools import lru_cache
from app.config.app_options import app_opt_get, load_fieldmap, app_opt_set
from .userfields import (
    detect_iban_userfield,
    detect_logical_userfield,
    get_deal_userfields,
    list_contact_userfields,
)
from app.validation.validate import detect_contact_iban_field_by_sampling

# =========================
# Auflösung: Deal-Mandat + Custom-IBAN-Codes
# =========================

def _resolve_field_codes(
    domain: str,
    token: str,
    category_id: int | None = None,
    *,
    include_portal_options: bool,
    include_fieldmap: bool,
    persist_detected_contact_iban: bool,
) -> dict:
    out = {}
    # Umgebungsvariablen
    for k, env_name in [
        ("DEBTOR_NAME", "B24_FIELD_DEBTOR_NAME"),
        ("MANDATE_ID", "B24_FIELD_MANDATE_ID"),
        ("MANDATE_DATE", "B24_FIELD_MANDATE_DATE"),
        ("CONTACT_IBAN", "B24_FIELD_CONTACT_IBAN"),
    ]:
        val = os.getenv(env_name)
        if val: out[k] = val

    if include_portal_options:
        for k in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE", "CONTACT_IBAN"):
            if not out.get(k):
                opt = app_opt_get(domain, token, f"FIELD_{k}")
                if opt:
                    out[k] = opt

    if include_fieldmap:
        fmap = load_fieldmap(domain, token)
        if category_id is not None:
            cat_key = f"cat_{category_id}"
            if fmap.get(cat_key):
                for k, v in fmap[cat_key].items():
                    if k in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE") and v and not out.get(k):
                        out[k] = v
        if fmap.get("default"):
            for k, v in fmap["default"].items():
                if k in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE") and v and not out.get(k):
                    out[k] = v

    # Mandats-Auto-Erkennung (Label-basiert)
    needed = [k for k in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE") if not out.get(k)]
    if needed:
        ufs = get_deal_userfields(domain, token)
        for logical in needed:
            guessed = detect_logical_userfield(ufs, logical)
            if guessed:
                out[logical] = guessed

    # Auto-Detect IBAN-Customfelder (Label-basiert oder Sampling)
    if not out.get("CONTACT_IBAN"):
        out["CONTACT_IBAN"] = detect_iban_userfield(list_contact_userfields(domain, token)) or out.get("CONTACT_IBAN")

    if not out.get("CONTACT_IBAN"):
        try:
            guessed = detect_contact_iban_field_by_sampling(domain, token, limit=400)
            if guessed:
                out["CONTACT_IBAN"] = guessed
                if persist_detected_contact_iban:
                    app_opt_set(domain, token, "FIELD_CONTACT_IBAN", guessed)
        except Exception:
            pass

    # Trim
    for k in list(out.keys()):
        if isinstance(out[k], str):
            out[k] = out[k].strip() or None

    return out


@lru_cache(maxsize=128)
def resolve_field_codes(domain: str, token: str, category_id: int | None = None) -> dict:
    return _resolve_field_codes(
        domain,
        token,
        category_id,
        include_portal_options=True,
        include_fieldmap=True,
        persist_detected_contact_iban=True,
    )


def scan_field_codes(domain: str, token: str, category_id: int | None = None) -> dict:
    return _resolve_field_codes(
        domain,
        token,
        category_id,
        include_portal_options=False,
        include_fieldmap=False,
        persist_detected_contact_iban=False,
    )
