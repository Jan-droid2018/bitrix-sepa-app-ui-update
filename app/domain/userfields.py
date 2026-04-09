import unicodedata
import time

from app.i18n import normalize_language, translate
from app.services.bitrix_helper import b24_batch, b24_call, b24_call_raw, b24_list_all, _chunk

SEPA_USERFIELD_SPECS = {
    "DEBTOR_NAME": {
        "scope": "deal",
        "method": "crm.deal.userfield.add",
        "field_name": "SEPA_DEBTOR",
        "xml_id": "SEPA_DEBTOR",
        "label": "SEPA Debitor-Name",
        "user_type_id": "string",
        "help": "Automatisch von der SEPA-App angelegt.",
    },
    "MANDATE_ID": {
        "scope": "deal",
        "method": "crm.deal.userfield.add",
        "field_name": "SEPA_MANDATE_ID",
        "xml_id": "SEPA_MANDATE_ID",
        "label": "SEPA Mandats-ID",
        "user_type_id": "string",
        "help": "Automatisch von der SEPA-App angelegt.",
    },
    "MANDATE_DATE": {
        "scope": "deal",
        "method": "crm.deal.userfield.add",
        "field_name": "SEPA_MANDATE_DATE",
        "xml_id": "SEPA_MANDATE_DATE",
        "label": "SEPA Mandatsdatum",
        "user_type_id": "date",
        "help": "Automatisch von der SEPA-App angelegt.",
    },
    "CONTACT_IBAN": {
        "scope": "contact",
        "method": "crm.contact.userfield.add",
        "field_name": "SEPA_IBAN",
        "xml_id": "SEPA_IBAN",
        "label": "SEPA IBAN",
        "user_type_id": "string",
        "help": "Automatisch von der SEPA-App angelegt.",
    },
}

USERFIELD_TRANSLATION_KEYS = {
    "DEBTOR_NAME": "userfield.debtor_name",
    "MANDATE_ID": "userfield.mandate_id",
    "MANDATE_DATE": "userfield.mandate_date",
    "CONTACT_IBAN": "userfield.contact_iban",
}

_USERFIELD_CACHE_TTL_SECONDS = 120
_userfield_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}


def _userfield_cache_key(domain: str, scope: str) -> tuple[str, str]:
    return (str(domain or "").strip().lower(), scope)


def _get_cached_userfields(domain: str, scope: str) -> list[dict] | None:
    cache_key = _userfield_cache_key(domain, scope)
    entry = _userfield_cache.get(cache_key)
    if entry is None:
        return None

    expires_at, payload = entry
    if time.time() >= expires_at:
        _userfield_cache.pop(cache_key, None)
        return None

    return [dict(userfield) for userfield in payload]


def _set_cached_userfields(domain: str, scope: str, payload: list[dict]):
    _userfield_cache[_userfield_cache_key(domain, scope)] = (
        time.time() + _USERFIELD_CACHE_TTL_SECONDS,
        [dict(userfield) for userfield in payload],
    )


def clear_userfield_cache(domain: str | None = None, *, scope: str | None = None):
    if domain is None:
        if scope is None:
            _userfield_cache.clear()
            return

        keys_to_remove = [
            cache_key
            for cache_key in _userfield_cache
            if cache_key[1] == scope
        ]
        for cache_key in keys_to_remove:
            _userfield_cache.pop(cache_key, None)
        return

    normalized_domain = str(domain or "").strip().lower()
    scopes = (scope,) if scope else ("deal", "contact")
    for current_scope in scopes:
        _userfield_cache.pop((normalized_domain, current_scope), None)

# =========================
# Userfields / Deals
# =========================


def get_deal_userfields(domain: str, access_token: str) -> list[dict]:
    cached = _get_cached_userfields(domain, "deal")
    if cached is not None:
        return cached

    base = b24_list_all(domain, access_token, "crm.deal.userfield.list") or []

    ids = [uf.get("ID") or uf.get("id") for uf in base if (uf.get("ID") or uf.get("id"))]
    out_map = {}

    for chunk in _chunk(ids, 50):
        commands = {str(fid): f"crm.deal.userfield.get?id={fid}" for fid in chunk}
        batch_res = b24_batch(domain, access_token, commands, halt=False)
        for key, value in batch_res.items():
            out_map[key] = value or {}

    out = []
    for uf in base:
        fid = str(uf.get("ID") or uf.get("id") or "")
        full = out_map.get(fid)
        out.append(full or uf)
    _set_cached_userfields(domain, "deal", out)
    return [dict(userfield) for userfield in out]


def list_contact_userfields(domain: str, access_token: str) -> list[dict]:
    cached = _get_cached_userfields(domain, "contact")
    if cached is not None:
        return cached

    base = b24_list_all(domain, access_token, "crm.contact.userfield.list") or []
    ids = [uf.get("ID") or uf.get("id") for uf in base if (uf.get("ID") or uf.get("id"))]
    out_map = {}

    for chunk in _chunk(ids, 50):
        commands = {str(fid): f"crm.contact.userfield.get?id={fid}" for fid in chunk}
        batch_res = b24_batch(domain, access_token, commands, halt=False)
        for key, value in batch_res.items():
            out_map[key] = value or {}

    out = []
    for uf in base:
        fid = str(uf.get("ID") or uf.get("id") or "")
        full = out_map.get(fid)
        out.append(full or uf)
    _set_cached_userfields(domain, "contact", out)
    return [dict(userfield) for userfield in out]


def _spec_for(logical_name: str) -> dict:
    try:
        return SEPA_USERFIELD_SPECS[logical_name]
    except KeyError as exc:
        raise KeyError(f"Unbekannte SEPA-Felddefinition: {logical_name}") from exc


def _userfield_translation_key(logical_name: str) -> str:
    try:
        return USERFIELD_TRANSLATION_KEYS[logical_name]
    except KeyError as exc:
        raise KeyError(f"Unbekannte SEPA-Felddefinition: {logical_name}") from exc


def get_sepa_userfield_label(logical_name: str, language: str = "de") -> str:
    lang = normalize_language(language)
    return translate(lang, f"{_userfield_translation_key(logical_name)}.label")


def get_sepa_userfield_help(logical_name: str, language: str = "de") -> str:
    lang = normalize_language(language)
    return translate(lang, f"{_userfield_translation_key(logical_name)}.help")


def _norm_token(value) -> str:
    return str(value or "").strip().upper()


def find_userfield_by_spec(userfields: list[dict], logical_name: str) -> dict | None:
    spec = _spec_for(logical_name)
    expected_field = _norm_token(spec["field_name"])
    expected_code = _norm_token(f"UF_CRM_{spec['field_name']}")
    expected_xml_id = _norm_token(spec["xml_id"])

    for userfield in userfields or []:
        field_name = _norm_token(userfield.get("FIELD_NAME"))
        xml_id = _norm_token(userfield.get("XML_ID"))
        if field_name in (expected_field, expected_code) or xml_id == expected_xml_id:
            return userfield
    return None


def ensure_sepa_userfield(
    domain: str,
    access_token: str,
    logical_name: str,
    userfields: list[dict] | None = None,
    *,
    language: str = "de",
):
    spec = _spec_for(logical_name)
    current_userfields = list(userfields or [])

    existing = find_userfield_by_spec(current_userfields, logical_name)
    if existing:
        return existing, False, current_userfields

    localized_label = get_sepa_userfield_label(logical_name, language)
    localized_help = get_sepa_userfield_help(logical_name, language)

    fields_payload = {
        "FIELD_NAME": spec["field_name"],
        "XML_ID": spec["xml_id"],
        "USER_TYPE_ID": spec["user_type_id"],
        "EDIT_FORM_LABEL": localized_label,
        "LIST_COLUMN_LABEL": localized_label,
        "LIST_FILTER_LABEL": localized_label,
        "HELP_MESSAGE": localized_help,
        "MULTIPLE": "N",
        "MANDATORY": "N",
        "SHOW_FILTER": "Y",
    }

    b24_call(domain, access_token, spec["method"], {"fields": fields_payload})
    clear_userfield_cache(domain, scope=spec["scope"])

    if spec["scope"] == "contact":
        refreshed = list_contact_userfields(domain, access_token) or []
    else:
        refreshed = get_deal_userfields(domain, access_token) or []

    created = find_userfield_by_spec(refreshed, logical_name)
    return created, True, refreshed


def get_deals_by_ids(domain: str, access_token: str, ids: list[str]):
    out = []
    for chunk in _chunk([str(item) for item in ids], 50):
        commands = {deal_id: f"crm.deal.get?id={deal_id}" for deal_id in chunk}
        batch_res = b24_batch(domain, access_token, commands, halt=False)
        for deal_id in chunk:
            out.append(batch_res.get(deal_id) or {"ID": deal_id})
    return out


def list_deals(domain: str, access_token: str, category_id=None, stage_id=None, limit=None, select_extra=None):
    base_select = ["ID", "TITLE", "OPPORTUNITY", "CURRENCY_ID", "COMPANY_ID", "CONTACT_ID", "CATEGORY_ID", "STAGE_ID"]
    select = base_select + (select_extra or [])
    filt = {"CHECK_PERMISSIONS": "Y"}
    if category_id is not None:
        filt["CATEGORY_ID"] = str(category_id)
    if stage_id not in (None, ""):
        filt["STAGE_ID"] = stage_id
    res = b24_list_all(domain, access_token, "crm.deal.list", {
        "order": {"ID": "DESC"},
        "filter": filt,
        "select": select
    }, max_items=limit)
    return res


def list_deals_page(domain, token, category_id=None, stage_id=None,
                    page: int = 1, page_size: int = 50, select_extra=None):
    """
    Holt genau eine Seite Deals (server-seitig), statt alle auf einmal.
    Bitrix liefert pro Request typischerweise nur bis zu 50 Elemente. Deshalb
    werden bei groesseren Seiten mehrere API-Aufrufe zusammengefuehrt.
    Gibt (rows, has_next) zurueck.
    """
    base_select = ["ID", "TITLE", "OPPORTUNITY", "CURRENCY_ID", "COMPANY_ID", "CONTACT_ID", "CATEGORY_ID", "STAGE_ID"]
    select = list(dict.fromkeys(base_select + (select_extra or [])))
    filt = {"CHECK_PERMISSIONS": "Y"}
    if category_id is not None:
        filt["CATEGORY_ID"] = str(category_id)
    if stage_id not in (None, ""):
        filt["STAGE_ID"] = stage_id

    page = max(1, int(page))
    page_size = max(1, min(200, int(page_size)))
    start = max(0, (page - 1) * page_size)
    target_size = page_size + 1

    rows = []
    next_start = start
    while next_start is not None and len(rows) < target_size:
        data = b24_call_raw(domain, token, "crm.deal.list", {
            "order": {"ID": "DESC"},
            "filter": filt,
            "select": select,
            "start": next_start
        })
        chunk = data.get("result", []) or []
        rows.extend(chunk)

        next_start = data.get("next")
        if not chunk:
            next_start = None

    has_next = len(rows) > page_size or next_start is not None
    return rows[:page_size], has_next


# =========================
# Feld-Erkennung / Label-Helfer / IBAN-Checker
# =========================


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    normalized = []
    last_was_space = False
    for ch in s.lower():
        if ch.isalnum():
            normalized.append(ch)
            last_was_space = False
        else:
            if not last_was_space:
                normalized.append(" ")
            last_was_space = True
    return "".join(normalized).strip()


def _compact_norm(s: str) -> str:
    return "".join(ch for ch in _norm(s) if ch.isalnum())


def _contains_needle(text: str, needle: str) -> bool:
    normalized_text = _norm(text)
    normalized_needle = _norm(needle)
    if not normalized_text or not normalized_needle:
        return False
    if normalized_needle in normalized_text:
        return True

    compact_text = _compact_norm(text)
    compact_needle = _compact_norm(needle)
    return bool(compact_needle and compact_needle in compact_text)


def _any_contains(text: str, needles: list[str]) -> bool:
    return any(_contains_needle(text, needle) for needle in needles)


FIELD_MATCHERS = {
    "DEBTOR_NAME": {
        "aliases": [
            "debitor",
            "debtor",
            "debtor name",
            "zahler",
            "zahlername",
            "name des zahlers",
            "zahlungspflichtiger",
            "zahlungspflichtige",
            "name des zahlungspflichtigen",
            "kontoinhaber",
            "kontoinhab",
            "kontoinhaber",
            "account holder",
            "payer",
            "payer name",
            "schuldner",
            "kunde",
            "kundenname",
        ],
        "allowed_types": ("string", "text"),
        "preferred_types": ("string", "text"),
        "blocked_terms": ("iban", "bic", "mandat", "referenz", "datum", "date"),
        "min_score": 7,
    },
    "MANDATE_ID": {
        "aliases": [
            "mandat id",
            "mandats id",
            "mandats-id",
            "mandatsnummer",
            "mandat nummer",
            "mandats nr",
            "mandatsnr",
            "mandate id",
            "mandate identifier",
            "mandate reference",
            "mandate ref",
            "mandatsreferenz",
            "mandats referenz",
            "mandatsreference",
            "mandatsreferens",
            "mandats referens",
            "mandatsreferenznummer",
            "mandatsreferensnummer",
            "sepa mandat",
            "sepa mandate",
            "sepa referenz",
            "sepa reference",
            "sepa ref",
            "reference mandate",
            "referenz mandate",
        ],
        "allowed_types": ("string", "text"),
        "preferred_types": ("string", "text"),
        "blocked_terms": ("datum", "date", "signed", "signature", "iban", "bic"),
        "min_score": 8,
    },
    "MANDATE_DATE": {
        "aliases": [
            "mandatsdatum",
            "mandat datum",
            "mandate date",
            "mandate signed on",
            "mandate signed at",
            "datum mandat",
            "datum des mandates",
            "datum der unterschrift",
            "unterschriftsdatum",
            "unterzeichnet am",
            "unterzeichnung am",
            "signaturdatum",
            "signature date",
            "date of signature",
            "signed on",
            "signed at",
            "signing date",
            "dt of sgntr",
        ],
        "allowed_types": ("date", "datetime", "string", "text"),
        "preferred_types": ("date", "datetime"),
        "blocked_terms": ("iban", "bic", "referenznummer", "referenz nr", "mandatsreferenz", "mandatsreferens"),
        "min_score": 8,
    },
}


def label_candidates(uf: dict) -> list[str]:
    labels = []
    for key in ("EDIT_FORM_LABEL", "LIST_COLUMN_LABEL", "LIST_FILTER_LABEL", "XML_ID", "HELP"):
        val = uf.get(key)
        if isinstance(val, dict):
            labels.extend(val.values())
        elif isinstance(val, str):
            labels.append(val)
    labels.append(uf.get("FIELD_NAME") or "")
    return [label for label in labels if label]


def detect_logical_userfield(userfields: list[dict], logical_name: str) -> str | None:
    matcher = FIELD_MATCHERS.get(logical_name) or {}
    aliases = matcher.get("aliases") or []
    allowed_types = tuple(matcher.get("allowed_types") or ())
    preferred_types = tuple(matcher.get("preferred_types") or ())
    blocked_terms = tuple(matcher.get("blocked_terms") or ())
    min_score = int(matcher.get("min_score") or 1)

    best_code = None
    best_score = 0

    for uf in userfields or []:
        code = (uf.get("FIELD_NAME") or "").strip()
        user_type = (uf.get("USER_TYPE_ID") or "").strip().lower()
        if not code.startswith("UF_CRM_"):
            continue
        if allowed_types and user_type not in allowed_types:
            continue

        labels = label_candidates(uf)
        score = 0

        for label in labels:
            label_score = 0
            normalized_label = _norm(label)
            compact_label = _compact_norm(label)

            for alias in aliases:
                if _contains_needle(label, alias):
                    normalized_alias = _norm(alias)
                    compact_alias = _compact_norm(alias)
                    label_score = max(label_score, 8)
                    if normalized_label == normalized_alias or compact_label == compact_alias:
                        label_score = max(label_score, 14)
                    elif compact_alias and compact_alias in compact_label:
                        label_score = max(label_score, 10)

            for blocked in blocked_terms:
                if _contains_needle(label, blocked):
                    label_score -= 6

            score = max(score, label_score)

        if user_type in preferred_types:
            score += 3

        if logical_name == "MANDATE_DATE" and user_type == "date":
            score += 3
        elif logical_name == "MANDATE_DATE" and user_type == "datetime":
            score += 2

        if score > best_score:
            best_score = score
            best_code = code

    return best_code if best_score >= min_score else None


def detect_iban_userfield(userfields):
    iban_aliases = [
        "iban",
        "konto iban",
        "bankverbindung",
        "kontoverbindung",
        "bankkonto",
        "bankdaten",
        "iban kontakt",
        "kontakt iban",
        "konto des zahlers",
    ]

    for uf in userfields or []:
        labels = label_candidates(uf)
        if any(_any_contains(label, iban_aliases) for label in labels):
            code = uf.get("FIELD_NAME") or ""
            if code.startswith("UF_CRM_"):
                return code
    return None
