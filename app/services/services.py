from app.domain.fields import resolve_field_codes
from .bitrix_helper import b24_list_all

# =========================
# Debtor-Infos NUR aus Kontakt/Firma (keine Requisites mehr)
# =========================

def _fullname_contact(row: dict) -> str:
    name = (row.get("NAME") or "").strip()
    last = (row.get("LAST_NAME") or "").strip()
    full = f"{name} {last}".strip()
    return full or name or last or ""


def bulk_debtor_info_for_deals(domain: str, token: str, deals: list[dict]) -> dict:
    contact_ids = sorted({int(d["CONTACT_ID"]) for d in deals if d.get("CONTACT_ID")})
    company_ids = sorted({int(d["COMPANY_ID"]) for d in deals if d.get("COMPANY_ID")})

    codes = resolve_field_codes(domain, token, None)
    contact_iban_code = codes.get("CONTACT_IBAN")

    # Kontakte vorladen
    contact_iban_by_id = {}
    contact_name_by_id = {}
    if contact_ids:
        sel = ["ID", "NAME", "LAST_NAME"]
        if contact_iban_code:
            sel.append(contact_iban_code)
        contacts = b24_list_all(domain, token, "crm.contact.list", {
            "filter": {"ID": contact_ids},
            "select": sel
        })
        for c in contacts or []:
            try:
                cid = int(c["ID"])
            except Exception:
                continue
            contact_name_by_id[cid] = _fullname_contact(c)
            if contact_iban_code:
                val = (c.get(contact_iban_code) or "").replace(" ", "")
                if val:
                    contact_iban_by_id[cid] = val

    # Firmen vorladen (nur Titel für Anzeige, keine IBAN)
    company_title_by_id = {}
    if company_ids:
        sel = ["ID", "TITLE"]
        companies = b24_list_all(domain, token, "crm.company.list", {
            "filter": {"ID": company_ids},
            "select": sel
        })
        for co in companies or []:
            try:
                cid = int(co["ID"])
            except Exception:
                continue
            company_title_by_id[cid] = (co.get("TITLE") or "").strip()

    info = {}
    for d in deals:
        did = str(d.get("ID"))
        iban = None
        contact_name = None
        company_title = None
        try:
            if d.get("CONTACT_ID"):
                eid = int(d["CONTACT_ID"])
                contact_name = contact_name_by_id.get(eid) or contact_name
                if eid in contact_iban_by_id:
                    iban = contact_iban_by_id[eid]
            if d.get("COMPANY_ID"):
                eid = int(d["COMPANY_ID"])
                company_title = company_title_by_id.get(eid) or company_title
        except Exception:
            pass
        info[did] = {
            "iban": iban,
            "bic": None,  # keine Requisites mehr
            "holder": None,  # keine Requisites mehr
            "contact_name": (contact_name or None),
            "company_title": (company_title or None),
        }
    return info


# =========================
# Hilfsfunktion für Feldlabels
# =========================
def _best_label(uf: dict, code: str) -> str:
    # 1. Bevorzugte Klartext-Felder aus Webhook
    for k in ["listLabel", "formLabel", "filterLabel", "title"]:
        if uf.get(k):
            val = uf[k]
            if isinstance(val, dict):
                return list(val.values())[0]
            return str(val)
    # 2. Klassische Bitrix Keys
    for k in ["LIST_COLUMN_LABEL", "LIST_FILTER_LABEL", "EDIT_FORM_LABEL"]:
        if uf.get(k):
            val = uf[k]
            if isinstance(val, dict):
                return list(val.values())[0]
            return str(val)
    # 3. Fallback: Code
    return code


