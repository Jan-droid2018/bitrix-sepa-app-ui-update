from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import re
import xml.etree.ElementTree as ET

# =========================
# Export pain.008
# =========================

PAIN_008_NS = "urn:iso:std:iso:20022:tech:xsd:pain.008.001.02"
INVALID_XML_TEXT_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _qn(tag: str) -> str:
    return f"{{{PAIN_008_NS}}}{tag}"


def _add(parent, tag: str, text: str | None = None, **attrs):
    elem = ET.SubElement(parent, _qn(tag), attrs)
    if text is not None:
        elem.text = text
    return elem


def _clean_text(value, max_length: int) -> str:
    text = " ".join(str(value or "").split())
    text = INVALID_XML_TEXT_RE.sub("", text)
    return text[:max_length]


def _amount(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def build_pain008_xml(creditor_name, creditor_iban, creditor_bic,
                      creditor_identifier, local_instrument, sequence_type,
                      payment_info_id, collection_date, tx_list) -> bytes:
    ET.register_namespace("", PAIN_008_NS)

    document = ET.Element(_qn("Document"))
    root = _add(document, "CstmrDrctDbtInitn")

    total_amount = sum((_amount(tx["amount"]) for tx in tx_list), Decimal("0.00"))

    grp_hdr = _add(root, "GrpHdr")
    _add(grp_hdr, "MsgId", _clean_text(payment_info_id, 35))
    _add(grp_hdr, "CreDtTm", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    _add(grp_hdr, "NbOfTxs", str(len(tx_list)))
    _add(grp_hdr, "CtrlSum", f"{total_amount:.2f}")
    initg_pty = _add(grp_hdr, "InitgPty")
    _add(initg_pty, "Nm", _clean_text(creditor_name, 70))

    pmt_inf = _add(root, "PmtInf")
    _add(pmt_inf, "PmtInfId", _clean_text(payment_info_id, 35))
    _add(pmt_inf, "PmtMtd", "DD")
    _add(pmt_inf, "BtchBookg", "true")
    _add(pmt_inf, "NbOfTxs", str(len(tx_list)))
    _add(pmt_inf, "CtrlSum", f"{total_amount:.2f}")
    pmt_tp_inf = _add(pmt_inf, "PmtTpInf")
    svc_lvl = _add(pmt_tp_inf, "SvcLvl")
    _add(svc_lvl, "Cd", "SEPA")
    lcl_instrm = _add(pmt_tp_inf, "LclInstrm")
    _add(lcl_instrm, "Cd", _clean_text(local_instrument, 4))
    _add(pmt_tp_inf, "SeqTp", _clean_text(sequence_type, 4))
    _add(pmt_inf, "ReqdColltnDt", collection_date.strftime("%Y-%m-%d"))

    cdtr = _add(pmt_inf, "Cdtr")
    _add(cdtr, "Nm", _clean_text(creditor_name, 70))
    cdtr_acct = _add(pmt_inf, "CdtrAcct")
    cdtr_acct_id = _add(cdtr_acct, "Id")
    _add(cdtr_acct_id, "IBAN", _clean_text(creditor_iban, 34))
    cdtr_agt = _add(pmt_inf, "CdtrAgt")
    fin_instn_id = _add(cdtr_agt, "FinInstnId")
    _add(fin_instn_id, "BIC", _clean_text(creditor_bic, 11))
    _add(pmt_inf, "ChrgBr", "SLEV")
    cdtr_schme_id = _add(pmt_inf, "CdtrSchmeId")
    identifier_id = _add(cdtr_schme_id, "Id")
    prvt_id = _add(identifier_id, "PrvtId")
    othr = _add(prvt_id, "Othr")
    _add(othr, "Id", _clean_text(creditor_identifier, 35))
    schme_nm = _add(othr, "SchmeNm")
    _add(schme_nm, "Prtry", "SEPA")

    for idx, tx in enumerate(tx_list, start=1):
        tx_elem = _add(pmt_inf, "DrctDbtTxInf")
        pmt_id = _add(tx_elem, "PmtId")
        _add(pmt_id, "EndToEndId", _clean_text(tx.get("end_to_end_id", f"E2E-{idx:05d}"), 35))
        _add(tx_elem, "InstdAmt", f"{_amount(tx['amount']):.2f}", Ccy="EUR")

        drct_dbt_tx = _add(tx_elem, "DrctDbtTx")
        mandate = _add(drct_dbt_tx, "MndtRltdInf")
        _add(mandate, "MndtId", _clean_text(tx["mandate_id"], 35))
        _add(mandate, "DtOfSgntr", _clean_text(tx["mandate_date"], 10))

        debtor = _add(tx_elem, "Dbtr")
        _add(debtor, "Nm", _clean_text(tx["debtor_name"], 70))
        debtor_acct = _add(tx_elem, "DbtrAcct")
        debtor_acct_id = _add(debtor_acct, "Id")
        _add(debtor_acct_id, "IBAN", _clean_text(tx["debtor_iban"], 34))

        remittance = _add(tx_elem, "RmtInf")
        _add(remittance, "Ustrd", _clean_text(tx.get("remittance", "Rechnung"), 140))

    return ET.tostring(document, encoding="utf-8", xml_declaration=True)
