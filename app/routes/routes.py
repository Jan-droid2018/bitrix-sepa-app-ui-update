import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from flask import Blueprint, jsonify, redirect, render_template, request, send_from_directory, session

from app.config.app_options import app_opt_get, app_opt_set
from app.domain.categories import _parse_category_id, list_categories
from app.domain.fields import resolve_field_codes
from app.domain.userfields import (
    get_deal_userfields,
    get_deals_by_ids,
    list_contact_userfields,
    list_deals_page,
)
from app.services.bitrix_helper import b24_call, get_auth_from_request
from app.services.export import build_pain008_xml
from app.services.services import _best_label, bulk_debtor_info_for_deals
from app.services.token_manager import expires_at_from_now, refresh_access_token
from app.services.upload import (
    _debug_log,
    ensure_company_sepa_folder,
    find_sepa_stage_id,
    upload_bytes_to_folder_verbose,
)
from app.validation.validate import (
    normalize_date_string,
    normalize_iban,
    normalize_mandate_id,
    validate_iban,
    validate_mandate_id,
)


main_bp = Blueprint("main", __name__)

AUTH_SESSION_KEYS = ("token", "domain", "member_id", "refresh_token", "expires_at")
ALLOWED_LCL_INSTR = {"CORE", "B2B"}
ALLOWED_SEQ = {"OOFF", "FRST", "RCUR", "FNAL"}
PAGE_SIZE_OPTIONS = (10, 25, 50, 100)
ASSETS_DIR = Path(__file__).resolve().parents[2] / "assets"


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _clear_auth_session():
    for key in AUTH_SESSION_KEYS:
        session.pop(key, None)


def _request_value(*names):
    for name in names:
        value = request.values.get(name)
        if value not in (None, ""):
            return value
    return None


def _store_auth_from_request() -> bool:
    token = _request_value("auth[access_token]", "AUTH_ID")
    domain = _request_value("auth[domain]", "DOMAIN")
    member_id = _request_value("auth[member_id]", "member_id")
    refresh_token = _request_value("auth[refresh_token]", "REFRESH_ID")
    expires_raw = _request_value("auth[expires]", "expires")

    has_direct_auth = bool(token and domain)
    if token:
        session["token"] = token
    if domain:
        session["domain"] = domain
    if member_id is not None:
        session["member_id"] = member_id
    if refresh_token:
        session["refresh_token"] = refresh_token
    if expires_raw:
        try:
            session["expires_at"] = expires_at_from_now(expires_raw)
        except Exception:
            session.pop("expires_at", None)

    return has_direct_auth


def _auth_params(domain: str | None, token: str | None, member_id: str | None, **extra) -> dict:
    params = {}
    if token:
        params["auth[access_token]"] = token
    if domain:
        params["auth[domain]"] = domain
    if member_id:
        params["auth[member_id]"] = member_id
    for key, value in extra.items():
        if value not in (None, ""):
            params[key] = value
    return params


def _with_auth_query(path: str, domain: str | None, token: str | None, member_id: str | None, **extra) -> str:
    params = _auth_params(domain, token, member_id, **extra)
    return f"{path}?{urlencode(params)}" if params else path


def _parse_int(value, default: int, *, min_value: int = 1, max_value: int | None = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default

    parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _normalize_stage_id(value) -> str | None:
    text = (value or "").strip()
    return text or None


def _dedupe_messages(messages: list[str]) -> list[str]:
    out = []
    seen = set()
    for msg in messages:
        if msg and msg not in seen:
            seen.add(msg)
            out.append(msg)
    return out


def _field_option_list(userfields: list[dict]) -> list[dict]:
    options = []
    for userfield in userfields:
        code = (userfield.get("FIELD_NAME") or "").strip()
        if not code.startswith("UF_CRM_"):
            continue

        raw_label = _best_label(userfield, code)
        label = str(raw_label).strip() if raw_label not in (None, "") else code
        options.append(
            {
                "code": code,
                "type": userfield.get("USER_TYPE_ID"),
                "label": label,
            }
        )

    return sorted(options, key=lambda item: (item["label"].lower(), item["code"]))


def _get_creditor_option(domain: str, token: str, name: str) -> str:
    return (app_opt_get(domain, token, name) or "").strip()


def _is_expired_token_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "access token provided has expired",
            "expired token",
            "expired_token",
            "invalid token",
            "token expired",
        )
    )


def get_domain_and_token():
    has_direct_auth = _store_auth_from_request()

    domain = session.get("domain")
    token = session.get("token")
    member_id = session.get("member_id")
    refresh_token = session.get("refresh_token")
    expires_at = _parse_int(session.get("expires_at"), 0, min_value=0)

    if not domain or not token:
        return None, None, None

    if not has_direct_auth and refresh_token and expires_at and time.time() >= expires_at:
        try:
            refreshed = refresh_access_token(domain, refresh_token)
        except Exception:
            _clear_auth_session()
            return None, None, None

        token = refreshed["access_token"]
        session["token"] = token
        if refreshed.get("refresh_token"):
            session["refresh_token"] = refreshed["refresh_token"]
        session["expires_at"] = expires_at_from_now(refreshed.get("expires_in"))

    return domain, token, member_id


def _load_categories(domain: str, token: str) -> list[dict]:
    return list_categories(domain, token)


def _build_index_context(domain: str | None, token: str | None, member_id: str | None) -> dict:
    page_size = _parse_int(
        request.values.get("page_size"),
        50,
        min_value=min(PAGE_SIZE_OPTIONS),
        max_value=max(PAGE_SIZE_OPTIONS),
    )
    if page_size not in PAGE_SIZE_OPTIONS:
        page_size = 50

    form_lcl_instr = (request.values.get("lcl_instr") or "CORE").strip().upper()
    if form_lcl_instr not in ALLOWED_LCL_INSTR:
        form_lcl_instr = "CORE"

    form_seq = (request.values.get("seq") or "OOFF").strip().upper()
    if form_seq not in ALLOWED_SEQ:
        form_seq = "OOFF"

    page = _parse_int(request.values.get("page"), 1, min_value=1)

    return {
        "access_token": token or "",
        "domain": domain or "",
        "member_id": member_id or "",
        "categories": [],
        "deals": [],
        "debtor_map": {},
        "selected_category_id": _parse_category_id(request.values.get("category_id")),
        "stage_id": _normalize_stage_id(request.values.get("stage_id")),
        "page": page,
        "page_size": page_size,
        "page_size_options": PAGE_SIZE_OPTIONS,
        "has_prev": page > 1,
        "has_next": False,
        "loaded_deals": 0,
        "today": _today_str(),
        "form_lcl_instr": form_lcl_instr,
        "form_seq": form_seq,
        "info_messages": [],
        "error_messages": [],
        "debtor_name_field": None,
        "mandate_id_field": None,
        "mandate_date_field": None,
    }


def _populate_index_listing(domain: str, token: str, context: dict):
    selected_category_id = context["selected_category_id"]
    stage_id = context["stage_id"]

    if selected_category_id is None and not stage_id:
        return

    field_codes = resolve_field_codes(domain, token, selected_category_id)
    select_extra = [
        code
        for code in dict.fromkeys([
            field_codes.get("DEBTOR_NAME"),
            field_codes.get("MANDATE_ID"),
            field_codes.get("MANDATE_DATE"),
        ])
        if code
    ]

    deals, has_next = list_deals_page(
        domain,
        token,
        selected_category_id,
        stage_id,
        page=context["page"],
        page_size=context["page_size"],
        select_extra=select_extra,
    )

    debtor_map = bulk_debtor_info_for_deals(domain, token, deals) if deals else {}
    info_messages = list(context.get("info_messages") or [])

    if not field_codes.get("MANDATE_ID"):
        info_messages.append(
            "Hinweis: Mandatsfeld ist nicht zugeordnet. Bitte in den Einstellungen das Deal-Feld setzen."
        )
    if debtor_map and all(not (value and value.get("iban")) for value in debtor_map.values()):
        info_messages.append(
            "Hinweis: Es konnte keine IBAN geladen werden. Bitte das Kontakt-IBAN-Feld in den Einstellungen setzen."
        )

    context.update(
        deals=deals,
        debtor_map=debtor_map,
        debtor_name_field=field_codes.get("DEBTOR_NAME"),
        mandate_id_field=field_codes.get("MANDATE_ID"),
        mandate_date_field=field_codes.get("MANDATE_DATE"),
        has_next=has_next,
        loaded_deals=len(deals),
        info_messages=_dedupe_messages(info_messages),
    )


def _render_index(domain: str, token: str, member_id: str | None, *, preserve_listing: bool = False,
                  info_messages: list[str] | None = None, error_messages: list[str] | None = None,
                  status_code: int = 200):
    context = _build_index_context(domain, token, member_id)
    context["info_messages"] = list(info_messages or [])
    context["error_messages"] = list(error_messages or [])

    try:
        context["categories"] = _load_categories(domain, token)
    except Exception as exc:
        context["error_messages"].append(f"Kategorien konnten nicht geladen werden: {exc}")
        return render_template("index.html", **context), 500

    if preserve_listing:
        try:
            _populate_index_listing(domain, token, context)
        except Exception as exc:
            context["error_messages"].append(f"Fehler beim Laden der Deals: {exc}")

    context["info_messages"] = _dedupe_messages(context["info_messages"])
    context["error_messages"] = _dedupe_messages(context["error_messages"])
    return render_template("index.html", **context), status_code


@main_bp.app_template_filter("regex_match")
def regex_match(s, pattern):
    return re.match(pattern, s or "") is not None


@main_bp.route("/", methods=["GET", "POST"])
def index():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    try:
        categories = _load_categories(domain, token)
    except Exception:
        return render_template("auth_bootstrap.html")

    context = _build_index_context(domain, token, member_id)
    context["categories"] = categories

    if os.getenv("B24_ENV") == "PROD":
        installed = app_opt_get(domain, token, "INSTALLED")
        if installed != "Y":
            return "App ist nicht installiert. Bitte über den Bitrix Marketplace installieren.", 403

    if request.args.get("refresh") in ("1", "true", "yes"):
        try:
            resolve_field_codes.cache_clear()
            context["info_messages"].append("Feld-Zuordnungen wurden neu geladen.")
        except Exception:
            pass

    if context["selected_category_id"] is None and not context["stage_id"]:
        context["info_messages"].append("Bitte Pipeline wählen. Daten werden erst danach geladen.")
        return render_template("index.html", **context)

    try:
        _populate_index_listing(domain, token, context)
    except Exception as exc:
        context["error_messages"].append(f"Fehler beim Laden der Deals: {exc}")

    context["info_messages"] = _dedupe_messages(context["info_messages"])
    context["error_messages"] = _dedupe_messages(context["error_messages"])
    return render_template("index.html", **context)


@main_bp.route("/settings", methods=["GET", "POST"])
def settings():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    saved = False
    error_messages = []
    info_messages = []

    autodetected = _parse_int(request.args.get("autodetected"), 0, min_value=0)
    if "autodetected" in request.args:
        if autodetected > 0:
            info_messages.append(f"Auto-Erkennung abgeschlossen. {autodetected} Feldzuordnung(en) gespeichert.")
        else:
            info_messages.append("Auto-Erkennung abgeschlossen. Es wurde keine passende Zuordnung gefunden.")

    field_name = app_opt_get(domain, token, "FIELD_DEBTOR_NAME") or ""
    field_mand_id = app_opt_get(domain, token, "FIELD_MANDATE_ID") or ""
    field_mand_date = app_opt_get(domain, token, "FIELD_MANDATE_DATE") or ""
    field_contact_iban = app_opt_get(domain, token, "FIELD_CONTACT_IBAN") or ""

    creditor_name = _get_creditor_option(domain, token, "CREDITOR_NAME")
    creditor_iban = _get_creditor_option(domain, token, "CREDITOR_IBAN")
    creditor_bic = _get_creditor_option(domain, token, "CREDITOR_BIC")
    creditor_ci = _get_creditor_option(domain, token, "CREDITOR_CI")

    if request.method == "POST":
        field_name = (request.form.get("field_name") or "").strip()
        field_mand_id = (request.form.get("field_mand_id") or "").strip()
        field_mand_date = (request.form.get("field_mand_date") or "").strip()
        field_contact_iban = (request.form.get("field_contact_iban") or "").strip()

        creditor_name = " ".join((request.form.get("creditor_name") or "").split())
        creditor_iban = (request.form.get("creditor_iban") or "").strip()
        creditor_bic = (request.form.get("creditor_bic") or "").strip()
        creditor_ci = (request.form.get("creditor_ci") or "").strip()

        if not creditor_name:
            error_messages.append("Bitte einen Gläubiger-Namen angeben.")

    try:
        deal_ufs = get_deal_userfields(domain, token) or []
    except Exception:
        deal_ufs = []
        error_messages.append("Deal-Felder konnten nicht geladen werden.")

    try:
        contact_ufs = list_contact_userfields(domain, token) or []
    except Exception:
        contact_ufs = []
        error_messages.append("Kontakt-Felder konnten nicht geladen werden.")

    deal_uf_codes = _field_option_list(deal_ufs)
    contact_uf_codes = _field_option_list(contact_ufs)

    if request.method == "POST" and not error_messages:
        field_save_failed = False
        for key, value in [
            ("FIELD_DEBTOR_NAME", field_name),
            ("FIELD_MANDATE_ID", field_mand_id),
            ("FIELD_MANDATE_DATE", field_mand_date),
            ("FIELD_CONTACT_IBAN", field_contact_iban),
        ]:
            try:
                app_opt_set(domain, token, key, value)
            except Exception:
                field_save_failed = True

        creditor_save_failed = False
        for key, value in [
            ("CREDITOR_NAME", creditor_name),
            ("CREDITOR_IBAN", creditor_iban),
            ("CREDITOR_BIC", creditor_bic),
            ("CREDITOR_CI", creditor_ci),
        ]:
            try:
                app_opt_set(domain, token, key, value)
            except Exception:
                creditor_save_failed = True

        resolve_field_codes.cache_clear()
        if field_save_failed or creditor_save_failed:
            if field_save_failed:
                error_messages.append("Die Feldzuordnungen konnten nicht in Bitrix24 gespeichert werden.")
            if creditor_save_failed:
                error_messages.append("Die Gläubigerdaten konnten nicht in Bitrix24 gespeichert werden.")
        else:
            saved = True
            info_messages.append("Einstellungen gespeichert.")

    return render_template(
        "settings.html",
        access_token=token,
        domain=domain,
        member_id=member_id,
        saved=saved,
        error_messages=_dedupe_messages(error_messages),
        info_messages=_dedupe_messages(info_messages),
        field_name=field_name,
        field_mand_id=field_mand_id,
        field_mand_date=field_mand_date,
        field_contact_iban=field_contact_iban,
        deal_uf_codes=deal_uf_codes,
        contact_uf_codes=contact_uf_codes,
        creditor_name=creditor_name,
        creditor_iban=creditor_iban,
        creditor_bic=creditor_bic,
        creditor_ci=creditor_ci,
    )


@main_bp.get("/debug_detect_mandate_fields")
def debug_detect_mandate_fields():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    try:
        resolve_field_codes.cache_clear()
        detected = resolve_field_codes(domain, token, None)

        saved = 0
        for logical_name in ("DEBTOR_NAME", "MANDATE_ID", "MANDATE_DATE", "CONTACT_IBAN"):
            code = detected.get(logical_name)
            if code:
                app_opt_set(domain, token, f"FIELD_{logical_name}", code)
                saved += 1

        resolve_field_codes.cache_clear()
        return redirect(_with_auth_query("/settings", domain, token, member_id, autodetected=saved))
    except Exception as exc:
        if _is_expired_token_error(exc):
            _clear_auth_session()
            return render_template("auth_bootstrap.html")
        raise


@main_bp.post("/export")
def export_pain008():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    debug_lines = []
    _debug_log(debug_lines, "Start export_pain008")

    creditor_name = " ".join(_get_creditor_option(domain, token, "CREDITOR_NAME").split())
    creditor_iban = _get_creditor_option(domain, token, "CREDITOR_IBAN")
    creditor_bic = _get_creditor_option(domain, token, "CREDITOR_BIC")
    ci = _get_creditor_option(domain, token, "CREDITOR_CI")

    config_errors = []
    if not (creditor_name and creditor_iban and creditor_bic and ci):
        config_errors.append("Bitte Gläubiger-Name, IBAN, BIC und Creditor Identifier (CI) in den Einstellungen ausfüllen.")
    if config_errors:
        return _render_index(domain, token, member_id, preserve_listing=True, error_messages=config_errors, status_code=400)

    lcl_instr = (request.form.get("lcl_instr") or "CORE").strip().upper()
    seq = (request.form.get("seq") or "OOFF").strip().upper()
    if lcl_instr not in ALLOWED_LCL_INSTR:
        lcl_instr = "CORE"
    if seq not in ALLOWED_SEQ:
        seq = "OOFF"

    exec_date_str = request.form.get("exec_date") or _today_str()
    deal_ids = request.form.getlist("deal_id")

    if not deal_ids:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=["Bitte mindestens einen Deal auswählen."],
            status_code=400,
        )

    normalized_exec_date = normalize_date_string(exec_date_str)
    if not normalized_exec_date:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=["Ungültiges Einzugsdatum."],
            status_code=400,
        )
    exec_date = datetime.strptime(normalized_exec_date, "%Y-%m-%d")

    try:
        deals = get_deals_by_ids(domain, token, deal_ids)
    except Exception as exc:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=[f"Fehler beim Laden der Deal-Details: {exc}"],
            status_code=500,
        )

    if not deals:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=["Es konnten keine gültigen Deals geladen werden."],
            status_code=400,
        )

    debtor_map = bulk_debtor_info_for_deals(domain, token, deals)
    tx_list = []
    tx_errors = []

    for deal in deals:
        cat_id = int(deal["CATEGORY_ID"]) if deal.get("CATEGORY_ID") else None
        codes = resolve_field_codes(domain, token, cat_id)

        try:
            amount = float(deal.get("OPPORTUNITY") or 0.0)
        except Exception:
            amount = 0.0

        debtor_info = debtor_map.get(str(deal.get("ID"))) or {}
        debtor_name = (
            debtor_info.get("contact_name")
            or debtor_info.get("company_title")
            or deal.get(codes.get("DEBTOR_NAME"))
            or deal.get("TITLE")
            or "Zahler"
        )
        debtor_iban = normalize_iban(debtor_info.get("iban") or "")
        mandate_id = normalize_mandate_id(deal.get(codes.get("MANDATE_ID")) or "")
        mandate_date = normalize_date_string(deal.get(codes.get("MANDATE_DATE")) or normalized_exec_date)

        deal_errors = []
        deal_prefix = f"Deal {deal.get('ID')}:"
        if amount <= 0:
            deal_errors.append(f"{deal_prefix} Betrag muss größer als 0 sein.")
        if not debtor_iban:
            deal_errors.append(f"{deal_prefix} IBAN wurde nicht gefunden.")
        elif not validate_iban(debtor_iban):
            deal_errors.append(f"{deal_prefix} IBAN ist ungültig.")
        if not mandate_id:
            deal_errors.append(f"{deal_prefix} Mandats-ID fehlt.")
        elif not validate_mandate_id(mandate_id):
            deal_errors.append(f"{deal_prefix} Mandats-ID ist ungültig.")
        if not mandate_date:
            deal_errors.append(f"{deal_prefix} Mandatsdatum fehlt oder ist ungültig.")

        if deal_errors:
            tx_errors.extend(deal_errors)
            continue

        tx_list.append({
            "amount": amount,
            "end_to_end_id": f"DEAL-{deal.get('ID')}",
            "debtor_name": debtor_name[:70],
            "debtor_iban": debtor_iban,
            "remittance": f"Deal {deal.get('ID')} - {deal.get('TITLE', '')}"[:140],
            "mandate_id": mandate_id,
            "mandate_date": mandate_date,
        })

    if tx_errors:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=tx_errors[:8],
            status_code=400,
        )

    payment_info_id = f"B24SDD-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    try:
        xml_bytes = build_pain008_xml(
            creditor_name,
            creditor_iban,
            creditor_bic,
            ci,
            lcl_instr,
            seq,
            payment_info_id,
            exec_date,
            tx_list,
        )
    except Exception as exc:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=[f"Fehler beim Erzeugen der SEPA-XML: {exc}"],
            status_code=500,
        )

    try:
        folder = ensure_company_sepa_folder(domain, token)
    except Exception as exc:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=[f"Zielordner für den Upload konnte nicht vorbereitet werden: {exc}"],
            status_code=500,
        )

    folder_id = folder.get("ID")
    filename = f"sepa_dd_{payment_info_id}.xml"
    upload_result = upload_bytes_to_folder_verbose(domain, token, folder_id, filename, xml_bytes, debug_lines)
    file_obj = upload_result.get("file") or {}
    file_url = file_obj.get("DOWNLOAD_URL") or file_obj.get("DETAIL_URL") or ""

    if not file_obj:
        upload_error = None
        for raw_name in ("raw_upload_step2", "raw_folder_upload", "raw_storage_upload"):
            raw = upload_result.get(raw_name) or {}
            upload_error = raw.get("error_description") or raw.get("error") or upload_error
            if upload_error:
                break
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=[upload_error or "Die SEPA-Datei konnte nicht in Bitrix Drive hochgeladen werden."],
            status_code=500,
        )

    src_cat_id = int(deals[0]["CATEGORY_ID"]) if deals and deals[0].get("CATEGORY_ID") else None
    sepa_stage_id = find_sepa_stage_id(domain, token, src_cat_id)

    new_deal_id = b24_call(domain, token, "crm.deal.add", {
        "fields": {
            "TITLE": f"SEPA Export {payment_info_id}",
            "CATEGORY_ID": src_cat_id,
            "STAGE_ID": sepa_stage_id,
        }
    })
    if not new_deal_id:
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=["Der SEPA-Deal konnte nicht erstellt werden."],
            status_code=500,
        )

    comment = f"SEPA-XML {filename} erstellt."
    if file_url:
        comment += f" <a href='{file_url}'>Download</a>"

    b24_call(domain, token, "crm.timeline.comment.add", {
        "fields": {
            "ENTITY_ID": int(new_deal_id),
            "ENTITY_TYPE_ID": 2,
            "COMMENT": comment,
        }
    })

    return _render_index(
        domain,
        token,
        member_id,
        preserve_listing=True,
        info_messages=[f"SEPA-Export erfolgreich. Neuer Auftrag #{new_deal_id} wurde erstellt."],
    )


@main_bp.route("/install", methods=["GET", "POST"])
def install():
    try:
        auth = get_auth_from_request(request)
    except Exception as exc:
        return f"Install fehlgeschlagen: {exc}", 400

    domain = auth["domain"]
    token = auth["access_token"]
    member_id = auth.get("member_id")

    session["token"] = token
    session["domain"] = domain
    if member_id:
        session["member_id"] = member_id
    if auth.get("refresh_token"):
        session["refresh_token"] = auth["refresh_token"]
    if auth.get("expires"):
        try:
            session["expires_at"] = expires_at_from_now(auth["expires"])
        except Exception:
            session.pop("expires_at", None)

    app_opt_set(domain, token, "INSTALLED", "Y")
    app_opt_set(domain, token, "APP_VERSION", "1.0.0")
    app_opt_set(domain, token, "INSTALLED_AT", datetime.now(timezone.utc).isoformat())

    return redirect(_with_auth_query("/", domain, token, member_id))


@main_bp.route("/uninstall", methods=["GET", "POST"])
def uninstall():
    try:
        auth = get_auth_from_request(request)
    except Exception:
        return jsonify({"status": "ok"})

    domain = auth["domain"]
    token = auth["access_token"]

    app_opt_set(domain, token, "INSTALLED", "N")
    _clear_auth_session()
    return jsonify({"status": "ok"})


@main_bp.get("/assets/<path:filename>")
def asset_file(filename: str):
    return send_from_directory(str(ASSETS_DIR), filename)
