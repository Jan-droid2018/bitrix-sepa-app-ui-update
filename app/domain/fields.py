import os
from functools import lru_cache
from app.config.app_options import app_opt_get, load_fieldmap, app_opt_set
from .userfields import (get_deal_userfields, FIELD_MATCHERS, label_candidates, detect_iban_userfield, 
                        list_contact_userfields, _any_contains)
from app.validation.validate import detect_contact_iban_field_by_sampling

# =========================
# Auflösung: Deal-Mandat + Custom-IBAN-Codes
# =========================

@lru_cache(maxsize=128)
def resolve_field_codes(domain: str, token: str, category_id: int | None = None) -> dict:
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

    # Portaloptionen
    for k in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE", "CONTACT_IBAN"):
        if not out.get(k):
            opt = app_opt_get(domain, token, f"FIELD_{k}")
            if opt: out[k] = opt

    # Kategorie-Map (nur Mandat/Name sinnvoll)
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
    needed = [k for k in ("MANDATE_ID", "MANDATE_DATE") if not out.get(k)]
    if needed:
        ufs = get_deal_userfields(domain, token)
        for logical, needles in FIELD_MATCHERS.items():
            if out.get(logical): continue
            for uf in ufs:
                labels = label_candidates(uf)
                if any(_any_contains(l, needles) for l in labels):
                    code = uf.get("FIELD_NAME") or ""
                    if code.startswith("UF_CRM_") and uf.get("USER_TYPE_ID") in ("string", "datetime", "date", "text"):
                        out[logical] = code
                        break

    # Auto-Detect IBAN-Customfelder (Label-basiert oder Sampling)
    if not out.get("CONTACT_IBAN"):
        out["CONTACT_IBAN"] = detect_iban_userfield(list_contact_userfields(domain, token)) or out.get("CONTACT_IBAN")

    if not out.get("CONTACT_IBAN"):
        try:
            guessed = detect_contact_iban_field_by_sampling(domain, token, limit=400)
            if guessed:
                out["CONTACT_IBAN"] = guessed
                app_opt_set(domain, token, "FIELD_CONTACT_IBAN", guessed)
        except Exception:
            pass

    # Trim
    for k in list(out.keys()):
        if isinstance(out[k], str):
            out[k] = out[k].strip() or None

    return out