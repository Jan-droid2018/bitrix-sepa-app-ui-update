import unicodedata

from app.services.bitrix_helper import b24_batch, b24_call_raw, b24_list_all, _chunk

# =========================
# Userfields / Deals
# =========================


def get_deal_userfields(domain: str, access_token: str) -> list[dict]:
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
    return out


def list_contact_userfields(domain: str, access_token: str) -> list[dict]:
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
    return out


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
    return s.lower().strip()


def _any_contains(text: str, needles: list[str]) -> bool:
    t = _norm(text)
    return any(n in t for n in needles)


FIELD_MATCHERS = {
    "DEBTOR_NAME": ["kontoinhaber", "kontoinhab", "zahler", "debitor", "debtor", "name des zahlers", "zahlungspflicht"],
    "MANDATE_ID": ["mandat", "mandatsref", "mandatsreferenz", "mandate id", "sepa ref", "sepa-referenz"],
    "MANDATE_DATE": ["mandatsdatum", "datum mand", "datum der unterschrift", "signature date", "unterschriftsdatum",
                     "dt of sgntr"],
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


def detect_iban_userfield(userfields):
    for uf in userfields or []:
        labels = label_candidates(uf)
        if any(_any_contains(label, ["iban", "bankverbindung", "konto iban"]) for label in labels):
            code = uf.get("FIELD_NAME") or ""
            if code.startswith("UF_CRM_"):
                return code
    return None
