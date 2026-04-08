import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from flask import Blueprint, jsonify, redirect, render_template, render_template_string, request, send_from_directory, session

from app.config.app_options import app_opt_get, app_opt_set
from app.domain.categories import _parse_category_id, list_categories
from app.domain.fields import resolve_field_codes, scan_field_codes
from app.domain.userfields import (
    SEPA_USERFIELD_SPECS,
    ensure_sepa_userfield,
    find_userfield_by_spec,
    get_sepa_userfield_label,
    get_deal_userfields,
    get_deals_by_ids,
    list_contact_userfields,
    list_deals_page,
)
from app.i18n import normalize_language, translate
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
from db import (
    can_export,
    create_user_if_not_exists,
    find_user_by_stripe_ids,
    get_user,
    increase_export,
    set_plan,
    set_stripe_ids,
)


main_bp = Blueprint("main", __name__)

AUTH_SESSION_KEYS = ("token", "domain", "member_id", "refresh_token", "expires_at")
LANG_SESSION_KEY = "app_lang"
LANG_OVERRIDE_SESSION_KEY = "app_lang_override"
PORTAL_LANG_SESSION_KEY = "portal_lang_code"
LANG_OVERRIDE_OPTION_NAME = "UI_LANGUAGE_OVERRIDE"
DEFAULT_LANGUAGE_MODE = "auto"
SUPPORTED_LANGUAGE_MODES = {DEFAULT_LANGUAGE_MODE, "de", "en"}
ALLOWED_LCL_INSTR = {"CORE", "B2B"}
ALLOWED_SEQ = {"OOFF", "FRST", "RCUR", "FNAL"}
PAGE_SIZE_OPTIONS = (10, 25, 50, 100)
ASSETS_DIR = Path(__file__).resolve().parents[2] / "assets"
FIELD_OPTION_NAMES = {
    "DEBTOR_NAME": "FIELD_DEBTOR_NAME",
    "MANDATE_ID": "FIELD_MANDATE_ID",
    "MANDATE_DATE": "FIELD_MANDATE_DATE",
    "CONTACT_IBAN": "FIELD_CONTACT_IBAN",
}
USERFIELD_SCOPE_LABELS = {
    "contact": "Kontakte",
    "deal": "Aufträge",
}


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


def _set_effective_session_language(value: str | None) -> str:
    normalized = normalize_language(value)
    session[LANG_SESSION_KEY] = normalized
    return normalized


def _normalize_language_override(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower().replace("_", "-")
    if not normalized:
        return None
    if normalized in SUPPORTED_LANGUAGE_MODES:
        return normalized
    return None


def _request_portal_language_value() -> str | None:
    return _request_value("auth[lang]", "lang")


def _request_page_language_value() -> str | None:
    return _request_value("app_lang")


def _request_language_override_value() -> str | None:
    return _normalize_language_override(_request_value("ui_lang"))


def _set_session_portal_language(value: str | None) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None

    session[PORTAL_LANG_SESSION_KEY] = raw
    return normalize_language(raw)


def _current_language_mode() -> str:
    return _normalize_language_override(session.get(LANG_OVERRIDE_SESSION_KEY)) or DEFAULT_LANGUAGE_MODE


def _resolve_effective_language(
    *,
    override: str | None = None,
    portal_language: str | None = None,
    request_language: str | None = None,
) -> str:
    if request_language not in (None, ""):
        return normalize_language(request_language)

    language_mode = _normalize_language_override(override) or _current_language_mode()
    if language_mode in ("de", "en"):
        return language_mode

    return normalize_language(portal_language or session.get(PORTAL_LANG_SESSION_KEY))


def _set_session_language_override(value: str | None) -> str:
    language_mode = _normalize_language_override(value) or DEFAULT_LANGUAGE_MODE
    session[LANG_OVERRIDE_SESSION_KEY] = language_mode
    _set_effective_session_language(
        _resolve_effective_language(
            override=language_mode,
            portal_language=session.get(PORTAL_LANG_SESSION_KEY),
        )
    )
    return language_mode


def _load_saved_language_override(domain: str | None, token: str | None) -> str:
    if not domain or not token:
        return DEFAULT_LANGUAGE_MODE
    return _normalize_language_override(app_opt_get(domain, token, LANG_OVERRIDE_OPTION_NAME)) or DEFAULT_LANGUAGE_MODE


def _hydrate_language_preferences(domain: str | None, token: str | None):
    portal_language = _request_portal_language_value()
    if portal_language not in (None, ""):
        _set_session_portal_language(portal_language)

    request_override = _request_language_override_value()
    if request_override is not None:
        session[LANG_OVERRIDE_SESSION_KEY] = request_override
    elif LANG_OVERRIDE_SESSION_KEY not in session:
        session[LANG_OVERRIDE_SESSION_KEY] = _load_saved_language_override(domain, token)
    else:
        session[LANG_OVERRIDE_SESSION_KEY] = _current_language_mode()

    _set_effective_session_language(
        _resolve_effective_language(
            override=session.get(LANG_OVERRIDE_SESSION_KEY),
            portal_language=session.get(PORTAL_LANG_SESSION_KEY),
        )
    )


def _current_language() -> str:
    language = _resolve_effective_language(
        override=session.get(LANG_OVERRIDE_SESSION_KEY),
        portal_language=_current_portal_language_code(),
        request_language=_request_page_language_value(),
    )
    if _request_page_language_value() in (None, ""):
        _set_effective_session_language(language)
    return language


def _current_portal_language_code() -> str:
    request_language = _request_portal_language_value()
    if request_language not in (None, ""):
        _set_session_portal_language(request_language)

    stored = str(session.get(PORTAL_LANG_SESSION_KEY) or "").strip().lower()
    if stored:
        return stored
    return normalize_language(session.get(LANG_SESSION_KEY))


def _tr(key: str, **kwargs) -> str:
    return translate(_current_language(), key, **kwargs)


def _scope_label(scope: str) -> str:
    return _tr(f"scope.{scope}")


def _sepa_field_label(logical_name: str) -> str:
    return get_sepa_userfield_label(logical_name, _current_language())


@main_bp.app_context_processor
def _inject_i18n_context():
    language = _current_language()
    return {
        "app_lang": language,
        "language_mode": _current_language_mode(),
        "language_next": _current_path_with_query(),
        "language_options": [
            {"value": DEFAULT_LANGUAGE_MODE, "label": translate(language, "language.auto")},
            {"value": "de", "label": translate(language, "language.german")},
            {"value": "en", "label": translate(language, "language.english")},
        ],
        "portal_lang_code": _current_portal_language_code(),
        "t": lambda key, **kwargs: translate(language, key, **kwargs),
    }


def _request_member_id(current_member_id: str | None = None) -> str | None:
    member_id = _request_value("auth[member_id]", "member_id")
    if member_id not in (None, ""):
        return member_id
    if current_member_id not in (None, ""):
        return current_member_id
    return session.get("member_id")


def _store_auth_from_request() -> bool:
    token = _request_value("auth[access_token]", "AUTH_ID")
    domain = _request_value("auth[domain]", "DOMAIN")
    member_id = _request_value("auth[member_id]", "member_id")
    refresh_token = _request_value("auth[refresh_token]", "REFRESH_ID")
    expires_raw = _request_value("auth[expires]", "expires")
    _set_session_portal_language(_request_portal_language_value())

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

    _hydrate_language_preferences(session.get("domain"), session.get("token"))

    return has_direct_auth


def _external_url(path: str) -> str:
    base_url = request.host_url.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def _import_stripe():
    try:
        import stripe
    except ImportError as exc:
        raise RuntimeError(_tr("stripe.error.library_missing")) from exc
    return stripe


def _get_stripe_api():
    stripe = _import_stripe()
    secret_key = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
    if not secret_key:
        raise RuntimeError(_tr("stripe.error.secret_key_missing"))
    stripe.api_key = secret_key
    return stripe


def _extract_member_id_from_stripe_object(obj: dict | None) -> str | None:
    payload = obj or {}

    metadata = payload.get("metadata") or {}
    member_id = (metadata.get("member_id") or "").strip()
    if member_id:
        return member_id

    subscription_details = payload.get("subscription_details") or {}
    metadata = subscription_details.get("metadata") or {}
    member_id = (metadata.get("member_id") or "").strip()
    if member_id:
        return member_id

    return None


def _resolve_member_id_from_stripe_object(
    obj: dict | None,
    *,
    fallback_subscription_id: str | None = None,
) -> str | None:
    member_id = _extract_member_id_from_stripe_object(obj)
    if member_id:
        return member_id

    payload = obj or {}
    user = find_user_by_stripe_ids(
        stripe_customer_id=payload.get("customer"),
        stripe_subscription_id=fallback_subscription_id or payload.get("subscription") or payload.get("id"),
    )
    if user:
        return user.get("member_id")

    return None


def _auth_params(domain: str | None, token: str | None, member_id: str | None, **extra) -> dict:
    params = {}
    if token:
        params["auth[access_token]"] = token
    if domain:
        params["auth[domain]"] = domain
    if member_id:
        params["auth[member_id]"] = member_id
    portal_lang_code = _current_portal_language_code()
    if portal_lang_code:
        params["auth[lang]"] = portal_lang_code
    for key, value in extra.items():
        if value not in (None, ""):
            params[key] = value
    return params


def _with_auth_query(path: str, domain: str | None, token: str | None, member_id: str | None, **extra) -> str:
    params = _auth_params(domain, token, member_id, **extra)
    return f"{path}?{urlencode(params)}" if params else path


def _current_path_with_query() -> str:
    query_items = [
        (key, value)
        for key, value in request.args.items(multi=True)
        if key not in {"app_lang", "ui_lang"}
    ]
    if not query_items:
        return request.path
    return f"{request.path}?{urlencode(query_items)}"


def _safe_next_path(value: str | None, default: str = "/settings") -> str:
    target = str(value or "").strip()
    if not target or not target.startswith("/") or target.startswith("//"):
        return default
    return target


def _persist_language_override(domain: str, token: str, language_mode: str):
    app_opt_set(domain, token, LANG_OVERRIDE_OPTION_NAME, language_mode.upper())


def _with_language_query(path: str) -> str:
    current_language = _current_language()
    if not current_language:
        return path
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}{urlencode({'app_lang': current_language})}"


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


def _billing_context(member_id: str | None) -> dict:
    member_id = (member_id or "").strip()
    if not member_id:
        return {
            "billing_member_id": "",
            "billing_plan": "free",
            "billing_plan_label": "Free",
            "billing_is_pro": False,
            "billing_exports_used": 0,
            "billing_exports_limit": 1,
            "billing_exports_limit_text": "1 Export / Monat",
            "billing_exports_remaining": 0,
            "billing_usage_text": "Member-ID fehlt. Billing-Status kann noch nicht geladen werden.",
            "billing_status_text": "Upgrade aktuell nicht verfügbar",
            "billing_checkout_enabled": False,
            "billing_can_export": False,
            "billing_limit_reached": False,
        }

    create_user_if_not_exists(member_id)
    user = get_user(member_id) or {}
    plan = (user.get("plan") or "free").strip().lower()
    is_pro = plan == "pro"
    exports_used = int(user.get("exports_used") or 0)
    export_limit = None if is_pro else 1
    exports_remaining = None if is_pro else max(0, 1 - exports_used)
    export_allowed = can_export(user)

    if is_pro:
        usage_text = "Pro Plan aktiv. Exporte sind unbegrenzt."
        status_text = "Unbegrenzte Exporte verfügbar"
        limit_text = "Unbegrenzt"
    else:
        usage_text = f"{exports_used} von 1 Export im aktuellen Monat verwendet."
        if exports_remaining == 1:
            status_text = "1 Export in diesem Monat verfügbar"
        elif exports_remaining == 0:
            status_text = "Monatslimit erreicht"
        else:
            status_text = f"{exports_remaining} Exporte verfügbar"
        limit_text = "1 Export / Monat"

    return {
        "billing_member_id": member_id,
        "billing_plan": plan,
        "billing_plan_label": "Pro" if is_pro else "Free",
        "billing_is_pro": is_pro,
        "billing_exports_used": exports_used,
        "billing_exports_limit": export_limit,
        "billing_exports_limit_text": limit_text,
        "billing_exports_remaining": exports_remaining,
        "billing_usage_text": usage_text,
        "billing_status_text": status_text,
        "billing_checkout_enabled": True,
        "billing_can_export": export_allowed,
        "billing_limit_reached": not export_allowed and not is_pro,
    }


def _billing_context(member_id: str | None) -> dict:
    member_id = (member_id or "").strip()
    if not member_id:
        return {
            "billing_member_id": "",
            "billing_plan": "free",
            "billing_plan_label": "Free",
            "billing_is_pro": False,
            "billing_exports_used": 0,
            "billing_exports_limit": 1,
            "billing_exports_limit_text": _tr("billing.limit.free"),
            "billing_exports_remaining": 0,
            "billing_usage_text": _tr("billing.status.member_missing"),
            "billing_status_text": _tr("billing.unavailable"),
            "billing_checkout_enabled": False,
            "billing_can_export": False,
            "billing_limit_reached": False,
        }

    create_user_if_not_exists(member_id)
    user = get_user(member_id) or {}
    plan = (user.get("plan") or "free").strip().lower()
    is_pro = plan == "pro"
    exports_used = int(user.get("exports_used") or 0)
    export_limit = None if is_pro else 1
    exports_remaining = None if is_pro else max(0, 1 - exports_used)
    export_allowed = can_export(user)

    if is_pro:
        usage_text = _tr("billing.usage.pro")
        status_text = _tr("billing.status.pro")
        limit_text = _tr("billing.limit.unlimited")
    else:
        usage_text = _tr("billing.usage.free", exports_used=exports_used)
        if exports_remaining == 1:
            status_text = _tr("billing.status.one_remaining")
        elif exports_remaining == 0:
            status_text = _tr("billing.status.limit_reached")
        else:
            status_text = _tr("billing.status.remaining", exports_remaining=exports_remaining)
        limit_text = _tr("billing.limit.free")

    return {
        "billing_member_id": member_id,
        "billing_plan": plan,
        "billing_plan_label": "Pro" if is_pro else "Free",
        "billing_is_pro": is_pro,
        "billing_exports_used": exports_used,
        "billing_exports_limit": export_limit,
        "billing_exports_limit_text": limit_text,
        "billing_exports_remaining": exports_remaining,
        "billing_usage_text": usage_text,
        "billing_status_text": status_text,
        "billing_checkout_enabled": True,
        "billing_can_export": export_allowed,
        "billing_limit_reached": not export_allowed and not is_pro,
    }


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


def _load_saved_field_options(domain: str, token: str) -> dict:
    return {
        logical_name: (app_opt_get(domain, token, option_name) or "").strip()
        for logical_name, option_name in FIELD_OPTION_NAMES.items()
    }

def _append_scope_feedback_messages(messages: list[str], prefix: str, logical_names: list[str]):
    for scope in ("deal", "contact"):
        labels = [
            SEPA_USERFIELD_SPECS[name]["label"]
            for name in logical_names
            if SEPA_USERFIELD_SPECS[name]["scope"] == scope
        ]
        if labels:
            messages.append(f"{prefix} {USERFIELD_SCOPE_LABELS[scope]}: {', '.join(labels)}.")


def _requested_field_options(saved_options: dict) -> dict:
    if request.method != "POST":
        return dict(saved_options)

    return {
        "DEBTOR_NAME": (request.form.get("field_name") or "").strip(),
        "MANDATE_ID": (request.form.get("field_mand_id") or "").strip(),
        "MANDATE_DATE": (request.form.get("field_mand_date") or "").strip(),
        "CONTACT_IBAN": (request.form.get("field_contact_iban") or "").strip(),
    }


def _render_settings_page(
    domain: str,
    token: str,
    member_id: str | None,
    *,
    saved: bool = False,
    info_messages: list[str] | None = None,
    error_messages: list[str] | None = None,
    field_values: dict | None = None,
    creditor_values: dict | None = None,
):
    field_values = dict(field_values or _load_saved_field_options(domain, token))
    creditor_values = dict(creditor_values or {
        "creditor_name": _get_creditor_option(domain, token, "CREDITOR_NAME"),
        "creditor_iban": _get_creditor_option(domain, token, "CREDITOR_IBAN"),
        "creditor_bic": _get_creditor_option(domain, token, "CREDITOR_BIC"),
        "creditor_ci": _get_creditor_option(domain, token, "CREDITOR_CI"),
    })

    page_error_messages = list(error_messages or [])

    try:
        deal_ufs = get_deal_userfields(domain, token) or []
    except Exception:
        deal_ufs = []
        page_error_messages.append("Deal-Felder konnten nicht geladen werden.")

    try:
        contact_ufs = list_contact_userfields(domain, token) or []
    except Exception:
        contact_ufs = []
        page_error_messages.append("Kontakt-Felder konnten nicht geladen werden.")

    return render_template(
        "settings.html",
        access_token=token,
        domain=domain,
        member_id=member_id,
        saved=saved,
        error_messages=_dedupe_messages(_translate_legacy_messages(page_error_messages)),
        info_messages=_dedupe_messages(_translate_legacy_messages(list(info_messages or []))),
        field_name=field_values.get("DEBTOR_NAME") or "",
        field_mand_id=field_values.get("MANDATE_ID") or "",
        field_mand_date=field_values.get("MANDATE_DATE") or "",
        field_contact_iban=field_values.get("CONTACT_IBAN") or "",
        deal_uf_codes=_field_option_list(deal_ufs),
        contact_uf_codes=_field_option_list(contact_ufs),
        creditor_name=creditor_values.get("creditor_name") or "",
        creditor_iban=creditor_values.get("creditor_iban") or "",
        creditor_bic=creditor_values.get("creditor_bic") or "",
        creditor_ci=creditor_values.get("creditor_ci") or "",
        **_billing_context(member_id),
    )


def _run_field_setup(domain: str, token: str, *, requested_options: dict | None = None) -> dict:
    info_messages = []
    error_messages = []
    saved_options = _load_saved_field_options(domain, token)
    requested_options = dict(requested_options or saved_options)
    missing_logical_names = [name for name, value in requested_options.items() if not value]

    applied_codes = {}
    for logical_name in FIELD_OPTION_NAMES:
        requested_value = (requested_options.get(logical_name) or "").strip()
        if requested_value:
            applied_codes[logical_name] = requested_value
        else:
            applied_codes[logical_name] = ""

    detected_logical_names = []
    linked_logical_names = []
    created_logical_names = []

    try:
        resolve_field_codes.cache_clear()
        detected = scan_field_codes(domain, token, None)
    except Exception as exc:
        if _is_expired_token_error(exc):
            raise
        detected = {}
        error_messages.append(f"Der Feld-Scan konnte nicht ausgeführt werden: {exc}")

    for logical_name in missing_logical_names:
        code = (detected.get(logical_name) or "").strip()
        if code:
            applied_codes[logical_name] = code
            detected_logical_names.append(logical_name)

    deal_userfields = []
    contact_userfields = []
    deal_lookup_failed = False
    contact_lookup_failed = False

    if any(
        not applied_codes.get(logical_name) and SEPA_USERFIELD_SPECS[logical_name]["scope"] == "deal"
        for logical_name in missing_logical_names
    ):
        try:
            deal_userfields = get_deal_userfields(domain, token) or []
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            deal_lookup_failed = True
            error_messages.append(f"Deal-Felder konnten nicht geladen werden: {exc}")

    if any(
        not applied_codes.get(logical_name) and SEPA_USERFIELD_SPECS[logical_name]["scope"] == "contact"
        for logical_name in missing_logical_names
    ):
        try:
            contact_userfields = list_contact_userfields(domain, token) or []
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            contact_lookup_failed = True
            error_messages.append(f"Kontakt-Felder konnten nicht geladen werden: {exc}")

    for logical_name in missing_logical_names:
        if applied_codes.get(logical_name):
            continue

        spec = SEPA_USERFIELD_SPECS[logical_name]
        scope = spec["scope"]
        current_userfields = contact_userfields if scope == "contact" else deal_userfields
        scope_lookup_failed = contact_lookup_failed if scope == "contact" else deal_lookup_failed

        if not scope_lookup_failed:
            existing = find_userfield_by_spec(current_userfields, logical_name)
            code = (existing or {}).get("FIELD_NAME") or ""
            code = code.strip()
            if code:
                applied_codes[logical_name] = code
                linked_logical_names.append(logical_name)
                continue

        try:
            ensured_field, was_created, refreshed_userfields = ensure_sepa_userfield(
                domain,
                token,
                logical_name,
                userfields=current_userfields,
            )
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            error_messages.append(
                f"Das Feld '{spec['label']}' konnte in {USERFIELD_SCOPE_LABELS[scope]} nicht automatisch angelegt werden: {exc}"
            )
            continue

        if scope == "contact":
            contact_userfields = refreshed_userfields
        else:
            deal_userfields = refreshed_userfields

        code = (ensured_field or {}).get("FIELD_NAME") or ""
        code = code.strip()
        if not code:
            error_messages.append(
                f"Das Feld '{spec['label']}' wurde erstellt, konnte aber danach nicht sauber gelesen werden."
            )
            continue

        applied_codes[logical_name] = code
        if was_created:
            created_logical_names.append(logical_name)
        else:
            linked_logical_names.append(logical_name)

    for logical_name, option_name in FIELD_OPTION_NAMES.items():
        previous = saved_options.get(logical_name) or ""
        current = applied_codes.get(logical_name) or ""
        if not current or current == previous:
            continue
        try:
            app_opt_set(domain, token, option_name, current)
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            error_messages.append(
                f"Die Feldzuordnung für '{SEPA_USERFIELD_SPECS[logical_name]['label']}' konnte nicht gespeichert werden: {exc}"
            )

    resolve_field_codes.cache_clear()

    if detected_logical_names or linked_logical_names or created_logical_names:
        info_messages.append("Auto-Erkennung wurde ausgeführt.")
    elif not missing_logical_names and not error_messages:
        info_messages.append("Auto-Erkennung wurde ausgeführt. Alle Feldzuordnungen sind bereits gesetzt.")
    elif not error_messages:
        info_messages.append("Auto-Erkennung wurde ausgeführt. Es wurde keine passende Feldzuordnung gefunden.")

    mapped_logical_names = detected_logical_names + linked_logical_names
    if mapped_logical_names:
        _append_scope_feedback_messages(
            info_messages,
            "Felder gefunden in",
            mapped_logical_names,
        )

    if created_logical_names:
        _append_scope_feedback_messages(
            info_messages,
            "Neue Felder automatisch angelegt in",
            created_logical_names,
        )
        info_messages.append(
            "Bitte die neu erstellten Felder in Bitrix24 bei Kontakte bzw. Aufträge in der Kartenansicht oder im Formular einblenden, falls sie dort noch nicht sichtbar sind."
        )

    return {
        "info_messages": _dedupe_messages(info_messages),
        "error_messages": _dedupe_messages(error_messages),
    }


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

    _hydrate_language_preferences(domain, token)
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

    context = {
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
    context.update(_billing_context(member_id))
    return context


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


def _translate_legacy_message(message: str) -> str:
    text = str(message or "")
    if _current_language() == "de" or not text:
        return text

    exact_matches = {
        "Feld-Zuordnungen wurden neu geladen.": _tr("index.info.fields_reloaded"),
        "Bitte Pipeline waehlen. Daten werden erst danach geladen.": _tr("index.info.select_pipeline"),
        "Bitte Pipeline wählen. Daten werden erst danach geladen.": _tr("index.info.select_pipeline"),
        "Bitte Pipeline wÃ¤hlen. Daten werden erst danach geladen.": _tr("index.info.select_pipeline"),
        "Bitte einen Gläubiger-Namen angeben.": _tr("settings.error.creditor_name_required"),
        "Bitte einen GlÃ¤ubiger-Namen angeben.": _tr("settings.error.creditor_name_required"),
        "Die Feldzuordnungen konnten nicht in Bitrix24 gespeichert werden.": _tr("settings.error.field_mappings_save_failed"),
        "Die Gläubigerdaten konnten nicht in Bitrix24 gespeichert werden.": _tr("settings.error.creditor_data_save_failed"),
        "Die GlÃ¤ubigerdaten konnten nicht in Bitrix24 gespeichert werden.": _tr("settings.error.creditor_data_save_failed"),
        "Einstellungen gespeichert.": _tr("settings.info.saved"),
        "Member-ID fehlt. Export kann nicht freigegeben werden.": _tr("export.error.member_id_missing"),
        "Free Plan erreicht: 1 Export pro Monat. Bitte auf Pro upgraden oder bis zum nächsten Monat warten.": _tr("export.error.free_limit_reached"),
        "Free Plan erreicht: 1 Export pro Monat. Bitte auf Pro upgraden oder bis zum nÃ¤chsten Monat warten.": _tr("export.error.free_limit_reached"),
        "Bitte Gläubiger-Name, IBAN, BIC und Creditor Identifier (CI) in den Einstellungen ausfüllen.": _tr("export.error.creditor_data_required"),
        "Bitte GlÃ¤ubiger-Name, IBAN, BIC und Creditor Identifier (CI) in den Einstellungen ausfÃ¼llen.": _tr("export.error.creditor_data_required"),
        "Bitte mindestens einen Deal auswählen.": _tr("export.error.no_deal_selected"),
        "Bitte mindestens einen Deal auswÃ¤hlen.": _tr("export.error.no_deal_selected"),
        "Ungültiges Einzugsdatum.": _tr("export.error.invalid_exec_date"),
        "UngÃ¼ltiges Einzugsdatum.": _tr("export.error.invalid_exec_date"),
        "Es konnten keine gültigen Deals geladen werden.": _tr("export.error.no_valid_deals"),
        "Es konnten keine gÃ¼ltigen Deals geladen werden.": _tr("export.error.no_valid_deals"),
        "Die SEPA-Datei konnte nicht in Bitrix Drive hochgeladen werden.": _tr("export.error.upload_file"),
    }
    if text in exact_matches:
        return exact_matches[text]

    pattern_handlers = [
        (
            r"^Auto-Erkennung abgeschlossen\. (\d+) Feldzuordnung\(en\) gespeichert\.$",
            lambda match: _tr("settings.info.autodetect_done", count=match.group(1)),
        ),
        (
            r"^Auto-Erkennung abgeschlossen\. Es wurde keine passende Zuordnung gefunden\.$",
            lambda match: _tr("settings.info.autodetect_none"),
        ),
        (
            r"^Kategorien konnten nicht geladen werden: (.+)$",
            lambda match: _tr("index.error.categories_load", error=match.group(1)),
        ),
        (
            r"^Fehler beim Laden der Deals: (.+)$",
            lambda match: _tr("index.error.deals_load", error=match.group(1)),
        ),
        (
            r"^Fehler beim Laden der Deal-Details: (.+)$",
            lambda match: _tr("export.error.deal_details_load", error=match.group(1)),
        ),
        (
            r"^Fehler beim Erzeugen der SEPA-XML: (.+)$",
            lambda match: _tr("export.error.xml_build", error=match.group(1)),
        ),
        (
            r"^Zielordner f(?:u|ü|Ã¼)r den Upload konnte nicht vorbereitet werden: (.+)$",
            lambda match: _tr("export.error.upload_folder", error=match.group(1)),
        ),
        (
            r"^SEPA-Export erfolgreich\. Neuer Auftrag #(\d+) wurde erstellt\.$",
            lambda match: _tr("export.success", deal_id=match.group(1)),
        ),
        (
            r"^Deal (\d+): Betrag muss gr(?:o|ö|Ã¶)sser als 0 sein\.$",
            lambda match: _tr(
                "export.error.amount_positive",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
        (
            r"^Deal (\d+): IBAN wurde nicht gefunden\.$",
            lambda match: _tr(
                "export.error.iban_missing",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
        (
            r"^Deal (\d+): IBAN ist ungu(?:e|ü|Ã¼)ltig\.$",
            lambda match: _tr(
                "export.error.iban_invalid",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
        (
            r"^Deal (\d+): Mandats-ID fehlt\.$",
            lambda match: _tr(
                "export.error.mandate_id_missing",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
        (
            r"^Deal (\d+): Mandats-ID ist ungu(?:e|ü|Ã¼)ltig\.$",
            lambda match: _tr(
                "export.error.mandate_id_invalid",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
        (
            r"^Deal (\d+): Mandatsdatum fehlt oder ist ungu(?:e|ü|Ã¼)ltig\.$",
            lambda match: _tr(
                "export.error.mandate_date_invalid",
                prefix=_tr("export.prefix.deal", deal_id=match.group(1)),
            ),
        ),
    ]

    for pattern, handler in pattern_handlers:
        match = re.match(pattern, text)
        if match:
            return handler(match)

    return text


def _translate_legacy_messages(messages: list[str]) -> list[str]:
    return [_translate_legacy_message(message) for message in messages]


def _append_scope_feedback_messages(messages: list[str], prefix_key: str, logical_names: list[str]):
    prefix = _tr(prefix_key)
    for scope in ("deal", "contact"):
        labels = [
            _sepa_field_label(name)
            for name in logical_names
            if SEPA_USERFIELD_SPECS[name]["scope"] == scope
        ]
        if labels:
            messages.append(f"{prefix} {_scope_label(scope)}: {', '.join(labels)}.")


def _render_settings_page(
    domain: str,
    token: str,
    member_id: str | None,
    *,
    saved: bool = False,
    info_messages: list[str] | None = None,
    error_messages: list[str] | None = None,
    field_values: dict | None = None,
    creditor_values: dict | None = None,
):
    field_values = dict(field_values or _load_saved_field_options(domain, token))
    creditor_values = dict(
        creditor_values
        or {
            "creditor_name": _get_creditor_option(domain, token, "CREDITOR_NAME"),
            "creditor_iban": _get_creditor_option(domain, token, "CREDITOR_IBAN"),
            "creditor_bic": _get_creditor_option(domain, token, "CREDITOR_BIC"),
            "creditor_ci": _get_creditor_option(domain, token, "CREDITOR_CI"),
        }
    )

    page_error_messages = list(error_messages or [])

    try:
        deal_ufs = get_deal_userfields(domain, token) or []
    except Exception:
        deal_ufs = []
        page_error_messages.append(_tr("settings.error.deal_fields_load"))

    try:
        contact_ufs = list_contact_userfields(domain, token) or []
    except Exception:
        contact_ufs = []
        page_error_messages.append(_tr("settings.error.contact_fields_load"))

    return render_template(
        "settings.html",
        access_token=token,
        domain=domain,
        member_id=member_id,
        saved=saved,
        error_messages=_dedupe_messages(page_error_messages),
        info_messages=_dedupe_messages(list(info_messages or [])),
        field_name=field_values.get("DEBTOR_NAME") or "",
        field_mand_id=field_values.get("MANDATE_ID") or "",
        field_mand_date=field_values.get("MANDATE_DATE") or "",
        field_contact_iban=field_values.get("CONTACT_IBAN") or "",
        deal_uf_codes=_field_option_list(deal_ufs),
        contact_uf_codes=_field_option_list(contact_ufs),
        creditor_name=creditor_values.get("creditor_name") or "",
        creditor_iban=creditor_values.get("creditor_iban") or "",
        creditor_bic=creditor_values.get("creditor_bic") or "",
        creditor_ci=creditor_values.get("creditor_ci") or "",
        **_billing_context(member_id),
    )


def _run_field_setup(domain: str, token: str, *, requested_options: dict | None = None) -> dict:
    info_messages = []
    error_messages = []
    saved_options = _load_saved_field_options(domain, token)
    requested_options = dict(requested_options or saved_options)
    missing_logical_names = [name for name, value in requested_options.items() if not value]

    applied_codes = {}
    for logical_name in FIELD_OPTION_NAMES:
        requested_value = (requested_options.get(logical_name) or "").strip()
        applied_codes[logical_name] = requested_value or ""

    detected_logical_names = []
    linked_logical_names = []
    created_logical_names = []

    try:
        resolve_field_codes.cache_clear()
        detected = scan_field_codes(domain, token, None)
    except Exception as exc:
        if _is_expired_token_error(exc):
            raise
        detected = {}
        error_messages.append(_tr("field_setup.error.scan_failed", error=exc))

    for logical_name in missing_logical_names:
        code = (detected.get(logical_name) or "").strip()
        if code:
            applied_codes[logical_name] = code
            detected_logical_names.append(logical_name)

    deal_userfields = []
    contact_userfields = []
    deal_lookup_failed = False
    contact_lookup_failed = False

    if any(
        not applied_codes.get(logical_name) and SEPA_USERFIELD_SPECS[logical_name]["scope"] == "deal"
        for logical_name in missing_logical_names
    ):
        try:
            deal_userfields = get_deal_userfields(domain, token) or []
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            deal_lookup_failed = True
            error_messages.append(_tr("field_setup.error.deal_fields_load", error=exc))

    if any(
        not applied_codes.get(logical_name) and SEPA_USERFIELD_SPECS[logical_name]["scope"] == "contact"
        for logical_name in missing_logical_names
    ):
        try:
            contact_userfields = list_contact_userfields(domain, token) or []
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            contact_lookup_failed = True
            error_messages.append(_tr("field_setup.error.contact_fields_load", error=exc))

    for logical_name in missing_logical_names:
        if applied_codes.get(logical_name):
            continue

        spec = SEPA_USERFIELD_SPECS[logical_name]
        scope = spec["scope"]
        current_userfields = contact_userfields if scope == "contact" else deal_userfields
        scope_lookup_failed = contact_lookup_failed if scope == "contact" else deal_lookup_failed

        if not scope_lookup_failed:
            existing = find_userfield_by_spec(current_userfields, logical_name)
            code = ((existing or {}).get("FIELD_NAME") or "").strip()
            if code:
                applied_codes[logical_name] = code
                linked_logical_names.append(logical_name)
                continue

        try:
            ensured_field, was_created, refreshed_userfields = ensure_sepa_userfield(
                domain,
                token,
                logical_name,
                userfields=current_userfields,
                language=_current_language(),
            )
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            error_messages.append(
                _tr(
                    "field_setup.error.auto_create_failed",
                    label=_sepa_field_label(logical_name),
                    scope=_scope_label(scope),
                    error=exc,
                )
            )
            continue

        if scope == "contact":
            contact_userfields = refreshed_userfields
        else:
            deal_userfields = refreshed_userfields

        code = ((ensured_field or {}).get("FIELD_NAME") or "").strip()
        if not code:
            error_messages.append(
                _tr(
                    "field_setup.error.created_but_unreadable",
                    label=_sepa_field_label(logical_name),
                )
            )
            continue

        applied_codes[logical_name] = code
        if was_created:
            created_logical_names.append(logical_name)
        else:
            linked_logical_names.append(logical_name)

    for logical_name, option_name in FIELD_OPTION_NAMES.items():
        previous = saved_options.get(logical_name) or ""
        current = applied_codes.get(logical_name) or ""
        if not current or current == previous:
            continue
        try:
            app_opt_set(domain, token, option_name, current)
        except Exception as exc:
            if _is_expired_token_error(exc):
                raise
            error_messages.append(
                _tr(
                    "field_setup.error.save_failed",
                    label=_sepa_field_label(logical_name),
                    error=exc,
                )
            )

    resolve_field_codes.cache_clear()

    if detected_logical_names or linked_logical_names or created_logical_names:
        info_messages.append(_tr("field_setup.info.ran"))
    elif not missing_logical_names and not error_messages:
        info_messages.append(_tr("field_setup.info.already_set"))
    elif not error_messages:
        info_messages.append(_tr("field_setup.info.none_found"))

    mapped_logical_names = detected_logical_names + linked_logical_names
    if mapped_logical_names:
        _append_scope_feedback_messages(info_messages, "field_setup.prefix.found", mapped_logical_names)

    if created_logical_names:
        _append_scope_feedback_messages(info_messages, "field_setup.prefix.created", created_logical_names)
        info_messages.append(_tr("field_setup.info.show_new_fields"))

    return {
        "info_messages": _dedupe_messages(info_messages),
        "error_messages": _dedupe_messages(error_messages),
    }


def _populate_index_listing(domain: str, token: str, context: dict):
    selected_category_id = context["selected_category_id"]
    stage_id = context["stage_id"]

    if selected_category_id is None and not stage_id:
        return

    field_codes = resolve_field_codes(domain, token, selected_category_id)
    select_extra = [
        code
        for code in dict.fromkeys(
            [
                field_codes.get("DEBTOR_NAME"),
                field_codes.get("MANDATE_ID"),
                field_codes.get("MANDATE_DATE"),
            ]
        )
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
        info_messages.append(_tr("index.hint.mandate_field_missing"))
    if debtor_map and all(not (value and value.get("iban")) for value in debtor_map.values()):
        info_messages.append(_tr("index.hint.iban_missing"))

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


def _render_index(
    domain: str,
    token: str,
    member_id: str | None,
    *,
    preserve_listing: bool = False,
    info_messages: list[str] | None = None,
    error_messages: list[str] | None = None,
    status_code: int = 200,
):
    context = _build_index_context(domain, token, member_id)
    context["info_messages"] = list(info_messages or [])
    context["error_messages"] = list(error_messages or [])

    try:
        context["categories"] = _load_categories(domain, token)
    except Exception as exc:
        context["error_messages"].append(_tr("index.error.categories_load", error=exc))
        return render_template("index.html", **context), 500

    if preserve_listing:
        try:
            _populate_index_listing(domain, token, context)
        except Exception as exc:
            context["error_messages"].append(_tr("index.error.deals_load", error=exc))

    context["info_messages"] = _dedupe_messages(_translate_legacy_messages(context["info_messages"]))
    context["error_messages"] = _dedupe_messages(_translate_legacy_messages(context["error_messages"]))
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
            return _tr("index.error.not_installed"), 403
            return "App ist nicht installiert. Bitte über den Bitrix Marketplace installieren.", 403

    if request.args.get("refresh") in ("1", "true", "yes"):
        try:
            resolve_field_codes.cache_clear()
            context["info_messages"].append("Feld-Zuordnungen wurden neu geladen.")
        except Exception:
            pass

    if context["selected_category_id"] is None and not context["stage_id"]:
        context["info_messages"].append("Bitte Pipeline wählen. Daten werden erst danach geladen.")
        context["info_messages"] = _dedupe_messages(_translate_legacy_messages(context["info_messages"]))
        context["error_messages"] = _dedupe_messages(_translate_legacy_messages(context["error_messages"]))
        return render_template("index.html", **context)

    try:
        _populate_index_listing(domain, token, context)
    except Exception as exc:
        context["error_messages"].append(f"Fehler beim Laden der Deals: {exc}")

    context["info_messages"] = _dedupe_messages(_translate_legacy_messages(context["info_messages"]))
    context["error_messages"] = _dedupe_messages(_translate_legacy_messages(context["error_messages"]))
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

    saved_field_options = _load_saved_field_options(domain, token)
    field_name = saved_field_options["DEBTOR_NAME"]
    field_mand_id = saved_field_options["MANDATE_ID"]
    field_mand_date = saved_field_options["MANDATE_DATE"]
    field_contact_iban = saved_field_options["CONTACT_IBAN"]

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

    return _render_settings_page(
        domain,
        token,
        member_id,
        saved=saved,
        info_messages=info_messages,
        error_messages=error_messages,
        field_values={
            "DEBTOR_NAME": field_name,
            "MANDATE_ID": field_mand_id,
            "MANDATE_DATE": field_mand_date,
            "CONTACT_IBAN": field_contact_iban,
        },
        creditor_values={
            "creditor_name": creditor_name,
            "creditor_iban": creditor_iban,
            "creditor_bic": creditor_bic,
            "creditor_ci": creditor_ci,
        },
    )


@main_bp.post("/set-language")
def set_language():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    language_mode = _normalize_language_override(request.form.get("ui_lang")) or DEFAULT_LANGUAGE_MODE
    _set_session_language_override(language_mode)

    try:
        _persist_language_override(domain, token, language_mode)
    except Exception:
        pass

    return redirect(
        _safe_next_path(
            request.form.get("next"),
            default=_with_auth_query("/settings", domain, token, member_id),
        )
    )


@main_bp.route("/debug_detect_mandate_fields", methods=["GET", "POST"])
def debug_detect_mandate_fields():
    domain, token, member_id = get_domain_and_token()
    if not token or not domain:
        return render_template("auth_bootstrap.html")

    try:
        result = _run_field_setup(
            domain,
            token,
            requested_options=_requested_field_options(_load_saved_field_options(domain, token)),
        )
        return _render_settings_page(
            domain,
            token,
            member_id,
            info_messages=result["info_messages"],
            error_messages=result["error_messages"],
        )
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

    member_id = _request_member_id(member_id)
    if not member_id:
        return _render_index(
            domain,
            token,
            None,
            preserve_listing=True,
            error_messages=["Member-ID fehlt. Export kann nicht freigegeben werden."],
            status_code=400,
        )

    create_user_if_not_exists(member_id)
    user = get_user(member_id)
    if not can_export(user):
        return _render_index(
            domain,
            token,
            member_id,
            preserve_listing=True,
            error_messages=["Free Plan erreicht: 1 Export pro Monat. Bitte auf Pro upgraden oder bis zum nächsten Monat warten."],
            status_code=403,
        )

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
            or _tr("export.fallback.payer")
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

    comment = _tr("export.timeline.comment_created", filename=filename)
    if file_url:
        comment += f" <a href='{file_url}'>{_tr('common.download')}</a>"

    b24_call(domain, token, "crm.timeline.comment.add", {
        "fields": {
            "ENTITY_ID": int(new_deal_id),
            "ENTITY_TYPE_ID": 2,
            "COMMENT": comment,
        }
    })

    increase_export(member_id)

    return _render_index(
        domain,
        token,
        member_id,
        preserve_listing=True,
        info_messages=[f"SEPA-Export erfolgreich. Neuer Auftrag #{new_deal_id} wurde erstellt."],
    )


@main_bp.post("/create-checkout-session")
def create_checkout_session():
    _store_auth_from_request()
    member_id = _request_member_id(session.get("member_id"))
    if not member_id:
        return jsonify({"error": _tr("stripe.checkout.error.member_id_missing")}), 400

    create_user_if_not_exists(member_id)
    user = get_user(member_id)
    if user and (user.get("plan") or "").strip().lower() == "pro":
        return jsonify({"error": _tr("stripe.checkout.error.pro_already_active")}), 400

    price_id = (os.getenv("STRIPE_PRICE_ID") or "").strip()
    if not price_id:
        return jsonify({"error": _tr("stripe.checkout.error.price_id_missing")}), 500

    try:
        stripe = _get_stripe_api()
        checkout_session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=_external_url(_with_language_query("/success")),
            cancel_url=_external_url(_with_language_query("/cancel")),
            client_reference_id=member_id,
            metadata={"member_id": member_id},
            subscription_data={"metadata": {"member_id": member_id}},
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    return jsonify({"id": checkout_session.id, "url": checkout_session.url})


@main_bp.post("/stripe/webhook")
def stripe_webhook():
    webhook_secret = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()
    if not webhook_secret:
        return jsonify({"error": _tr("stripe.webhook.error.secret_missing")}), 500

    try:
        stripe = _import_stripe()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    payload = request.get_data()
    signature = request.headers.get("Stripe-Signature", "")
    if not signature:
        return jsonify({"error": _tr("stripe.webhook.error.signature_missing")}), 400

    try:
        event = stripe.Webhook.construct_event(payload, signature, webhook_secret)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    event_type = event.get("type")
    data_object = ((event.get("data") or {}).get("object")) or {}

    if event_type == "checkout.session.completed":
        member_id = _resolve_member_id_from_stripe_object(
            data_object,
            fallback_subscription_id=data_object.get("subscription"),
        )
        if member_id:
            set_plan(member_id, "pro")
            set_stripe_ids(member_id, data_object.get("customer"), data_object.get("subscription"))

    elif event_type == "customer.subscription.deleted":
        member_id = _resolve_member_id_from_stripe_object(
            data_object,
            fallback_subscription_id=data_object.get("id"),
        )
        if member_id:
            current_user = get_user(member_id) or {}
            set_plan(member_id, "free")
            set_stripe_ids(
                member_id,
                data_object.get("customer") or current_user.get("stripe_customer_id"),
                None,
            )

    elif event_type == "invoice.payment_failed":
        member_id = _resolve_member_id_from_stripe_object(
            data_object,
            fallback_subscription_id=data_object.get("subscription"),
        )
        if member_id:
            current_user = get_user(member_id) or {}
            set_plan(member_id, "free")
            set_stripe_ids(
                member_id,
                data_object.get("customer") or current_user.get("stripe_customer_id"),
                None,
            )

    return jsonify({"status": "ok"})


def _render_checkout_status_page(title_key: str, heading_key: str, body_key: str):
    return render_template_string(
        """
        <!doctype html>
        <html lang="{{ html_lang }}">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width,initial-scale=1"/>
                <title>{{ title }}</title>
            </head>
            <body>
                <h1>{{ heading }}</h1>
                <p>{{ body }}</p>
            </body>
        </html>
        """,
        html_lang=_current_language(),
        title=_tr(title_key),
        heading=_tr(heading_key),
        body=_tr(body_key),
    )


@main_bp.get("/success")
def checkout_success():
    return _render_checkout_status_page(
        "checkout.success.title",
        "checkout.success.heading",
        "checkout.success.body",
    )


@main_bp.get("/cancel")
def checkout_cancel():
    return _render_checkout_status_page(
        "checkout.cancel.title",
        "checkout.cancel.heading",
        "checkout.cancel.body",
    )


@main_bp.route("/install", methods=["GET", "POST"])
def install():
    try:
        auth = get_auth_from_request(request)
    except Exception as exc:
        return _tr("install.error", error=exc), 400

    domain = auth["domain"]
    token = auth["access_token"]
    member_id = auth.get("member_id")

    session["token"] = token
    session["domain"] = domain
    if member_id:
        session["member_id"] = member_id
    session.pop(LANG_OVERRIDE_SESSION_KEY, None)
    _set_session_portal_language(auth.get("lang"))
    _set_effective_session_language(
        _resolve_effective_language(
            override=DEFAULT_LANGUAGE_MODE,
            portal_language=auth.get("lang"),
        )
    )
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
