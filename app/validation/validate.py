from datetime import date, datetime
import re
import string

from app.domain.userfields import list_contact_userfields
from app.services.bitrix_helper import b24_call_raw

# --- IBAN-Erkennung ueber Inhalte ---
IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$")


def normalize_iban(iban: str) -> str:
    return re.sub(r"\s+", "", (iban or "")).upper()


def looks_like_iban(val: str) -> bool:
    if not val:
        return False
    normalized = normalize_iban(val)
    if any(ch not in string.ascii_uppercase + string.digits for ch in normalized):
        return False
    return bool(IBAN_RE.match(normalized)) and 15 <= len(normalized) <= 34


def validate_iban(iban: str) -> bool:
    """Strenge Validierung nach ISO 13616 (Modulo-97)."""
    if not looks_like_iban(iban):
        return False
    normalized = normalize_iban(iban)
    rearranged = normalized[4:] + normalized[:4]
    digits = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in rearranged)
    try:
        return int(digits) % 97 == 1
    except Exception:
        return False


# --- BIC / CI / Mandats-ID ---
BIC_RE = re.compile(r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$")
CI_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{3,28}$")
MANDATE_ID_ALLOWED_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_/\. ]{0,34}$")


def _clean_bic(v: str) -> str:
    v = (v or "").upper()
    v = v.replace("\u00A0", "")
    v = re.sub(r"[\s\-_]+", "", v)
    return v


def normalize_bic(v: str) -> str:
    return _clean_bic(v)


def validate_bic(v: str) -> bool:
    normalized = normalize_bic(v)
    if len(normalized) not in (8, 11):
        return False
    return bool(BIC_RE.match(normalized))


def normalize_ci(v: str) -> str:
    return re.sub(r"\s+", "", (v or "").strip().upper())


def validate_ci(v: str) -> bool:
    normalized = normalize_ci(v)
    return len(normalized) <= 35 and bool(CI_RE.match(normalized))


def normalize_mandate_id(v: str) -> str:
    return re.sub(r"\s+", " ", (v or "").strip())


def validate_mandate_id(v: str) -> bool:
    normalized = normalize_mandate_id(v)
    return bool(MANDATE_ID_ALLOWED_RE.match(normalized))


def normalize_date_string(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    raw = str(value).strip()
    if not raw:
        return None

    candidates = []
    normalized = raw.replace("/", "-")
    candidates.append(normalized)
    if "T" in normalized:
        candidates.append(normalized.split("T", 1)[0])
    if " " in normalized:
        candidates.append(normalized.split(" ", 1)[0])

    for candidate in candidates:
        try:
            return datetime.strptime(candidate, "%Y-%m-%d").date().isoformat()
        except ValueError:
            pass

    for fmt in ("%d.%m.%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass

    return None


def detect_contact_iban_field_by_sampling(domain: str, token: str, limit: int = 400) -> str | None:
    ufs = list_contact_userfields(domain, token) or []
    uf_codes = [
        uf.get("FIELD_NAME")
        for uf in ufs
        if (
            uf.get("FIELD_NAME", "").startswith("UF_CRM_")
            and uf.get("USER_TYPE_ID") in (None, "string", "crm", "text")
        )
    ]
    if not uf_codes:
        return None

    hits_by_field = {code: 0 for code in uf_codes}
    ids_seen = 0
    start = 0
    while ids_seen < limit:
        data = b24_call_raw(domain, token, "crm.contact.list", {
            "order": {"ID": "DESC"},
            "filter": {},
            "select": ["ID"] + uf_codes,
            "start": start
        })
        rows = data.get("result", []) or []
        if not rows:
            break

        for row in rows:
            ids_seen += 1
            for code in uf_codes:
                value = row.get(code)
                if isinstance(value, str) and looks_like_iban(value):
                    hits_by_field[code] += 1
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        if isinstance(item, str) and looks_like_iban(item):
                            hits_by_field[code] += 1
                            break
                elif isinstance(value, dict):
                    candidate = value.get("value") or value.get("VALUE") or ""
                    if isinstance(candidate, str) and looks_like_iban(candidate):
                        hits_by_field[code] += 1

        if data.get("next") is None:
            break
        start = data.get("next")
        if ids_seen >= limit:
            break

    best_code, best_hits = None, 0
    for code, hits in hits_by_field.items():
        if hits > best_hits:
            best_code, best_hits = code, hits
    return best_code if best_hits > 0 else None
