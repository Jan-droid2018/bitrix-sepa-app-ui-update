import os
from datetime import datetime
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse
import xml.etree.ElementTree as ET

from flask import session

os.environ.setdefault("BITRIX_CLIENT_ID", "test-client")
os.environ.setdefault("BITRIX_CLIENT_SECRET", "test-secret")

from app import create_app
from app.config.app_options import app_opt_get, app_opt_get_many, app_opt_set, clear_app_option_cache
from app.domain.categories import clear_category_cache
from app.domain.userfields import (
    clear_userfield_cache,
    detect_iban_userfield,
    detect_logical_userfield,
    ensure_sepa_userfield,
    list_deals_page,
)
from app.services.bitrix_helper import b24_call_raw
from app.services.export import PAIN_008_NS, build_pain008_xml


def _clear_runtime_caches():
    clear_app_option_cache()
    clear_category_cache()
    clear_userfield_cache()


class AppOptionsTests(unittest.TestCase):
    def setUp(self):
        _clear_runtime_caches()

    def test_app_opt_set_uses_options_payload(self):
        with patch("app.config.app_options.b24_call") as mocked_call:
            app_opt_set("example.bitrix24.de", "token-1", "CREDITOR_NAME", "Portal Creditor")

        mocked_call.assert_called_once_with(
            "example.bitrix24.de",
            "token-1",
            "app.option.set",
            {"options": {"SEPA_SDD_CREDITOR_NAME": "Portal Creditor"}},
        )

    def test_app_opt_get_handles_legacy_option_value_shape(self):
        legacy_payload = {
            "option": "SEPA_SDD_CREDITOR_NAME",
            "value": "Portal Creditor",
        }

        with patch("app.config.app_options.b24_call", return_value=legacy_payload):
            result = app_opt_get("example.bitrix24.de", "token-1", "CREDITOR_NAME")

        self.assertEqual(result, "Portal Creditor")

    def test_app_opt_get_many_batches_requested_options(self):
        batch_payload = {
            "CREDITOR_NAME": {"SEPA_SDD_CREDITOR_NAME": "Portal Creditor"},
            "CREDITOR_IBAN": {"SEPA_SDD_CREDITOR_IBAN": "DE123"},
        }

        with patch("app.config.app_options.b24_batch", return_value=batch_payload) as mocked_batch:
            result = app_opt_get_many(
                "example.bitrix24.de",
                "token-1",
                ("CREDITOR_NAME", "CREDITOR_IBAN"),
            )

        self.assertEqual(result["CREDITOR_NAME"], "Portal Creditor")
        self.assertEqual(result["CREDITOR_IBAN"], "DE123")
        mocked_batch.assert_called_once()


class ExportTests(unittest.TestCase):
    def test_build_pain008_xml_uses_default_namespace(self):
        xml_bytes = build_pain008_xml(
            "Creditor GmbH",
            "DE02120300000000202051",
            "BYLADEM1001",
            "DE98ZZZ09999999999",
            "CORE",
            "OOFF",
            "TEST123",
            datetime(2026, 3, 23),
            [{
                "amount": 10.5,
                "debtor_name": "Max Mustermann",
                "debtor_iban": "DE02120300000000202051",
                "mandate_id": "MANDAT1",
                "mandate_date": "2026-03-01",
                "end_to_end_id": "E2E1",
                "remittance": "Test",
            }],
        )

        root = ET.fromstring(xml_bytes)
        self.assertEqual(root.tag, f"{{{PAIN_008_NS}}}Document")


class PagingTests(unittest.TestCase):
    def test_list_deals_page_spans_multiple_bitrix_pages(self):
        responses = [
            {
                "result": [{"ID": str(idx)} for idx in range(50)],
                "next": 50,
            },
            {
                "result": [{"ID": str(idx)} for idx in range(50, 100)],
                "next": 100,
            },
        ]

        with patch("app.domain.userfields.b24_call_raw", side_effect=responses) as mocked_call:
            rows, has_next = list_deals_page(
                "example.bitrix24.de",
                "token",
                page=1,
                page_size=75,
            )

        self.assertEqual(len(rows), 75)
        self.assertEqual(rows[0]["ID"], "0")
        self.assertEqual(rows[-1]["ID"], "74")
        self.assertTrue(has_next)
        self.assertEqual(mocked_call.call_count, 2)


class UserfieldProvisioningTests(unittest.TestCase):
    def test_ensure_sepa_userfield_uses_existing_field_without_duplicate_create(self):
        existing = {
            "FIELD_NAME": "UF_CRM_SEPA_IBAN",
            "XML_ID": "SEPA_IBAN",
        }

        with patch("app.domain.userfields.b24_call") as mocked_call:
            userfield, created, refreshed = ensure_sepa_userfield(
                "example.bitrix24.de",
                "token-1",
                "CONTACT_IBAN",
                userfields=[existing],
            )

        self.assertEqual(userfield, existing)
        self.assertFalse(created)
        self.assertEqual(refreshed, [existing])
        mocked_call.assert_not_called()

    def test_ensure_sepa_userfield_creates_missing_field_and_reloads_entity_fields(self):
        created_field = {
            "FIELD_NAME": "UF_CRM_SEPA_MANDATE_ID",
            "XML_ID": "SEPA_MANDATE_ID",
        }

        with patch("app.domain.userfields.b24_call") as mocked_call, \
             patch("app.domain.userfields.get_deal_userfields", return_value=[created_field]):
            userfield, created, refreshed = ensure_sepa_userfield(
                "example.bitrix24.de",
                "token-1",
                "MANDATE_ID",
                userfields=[],
            )

        self.assertEqual(userfield, created_field)
        self.assertTrue(created)
        self.assertEqual(refreshed, [created_field])
        mocked_call.assert_called_once()

    def test_ensure_sepa_userfield_uses_english_labels_when_requested(self):
        with patch("app.domain.userfields.b24_call") as mocked_call, \
             patch("app.domain.userfields.get_deal_userfields", return_value=[{
                 "FIELD_NAME": "UF_CRM_SEPA_MANDATE_ID",
                 "XML_ID": "SEPA_MANDATE_ID",
             }]):
            ensure_sepa_userfield(
                "example.bitrix24.de",
                "token-1",
                "MANDATE_ID",
                userfields=[],
                language="en",
            )

        payload = mocked_call.call_args.args[3]["fields"]
        self.assertEqual(payload["EDIT_FORM_LABEL"], "SEPA Mandate ID")
        self.assertEqual(payload["HELP_MESSAGE"], "Created automatically by the SEPA app.")


class UserfieldDetectionTests(unittest.TestCase):
    def test_detect_logical_userfield_handles_synonyms_without_confusing_date_and_id(self):
        userfields = [
            {
                "FIELD_NAME": "UF_CRM_MANDATE_DATE",
                "USER_TYPE_ID": "date",
                "EDIT_FORM_LABEL": "Mandatsdatum",
            },
            {
                "FIELD_NAME": "UF_CRM_MANDATE_REF",
                "USER_TYPE_ID": "string",
                "EDIT_FORM_LABEL": "Mandatsreferens",
            },
        ]

        self.assertEqual(detect_logical_userfield(userfields, "MANDATE_ID"), "UF_CRM_MANDATE_REF")
        self.assertEqual(detect_logical_userfield(userfields, "MANDATE_DATE"), "UF_CRM_MANDATE_DATE")

    def test_detect_iban_userfield_accepts_alternative_contact_labels(self):
        userfields = [
            {
                "FIELD_NAME": "UF_CRM_CONTACT_BANK",
                "USER_TYPE_ID": "string",
                "EDIT_FORM_LABEL": "Kontoverbindung",
            }
        ]

        self.assertEqual(detect_iban_userfield(userfields), "UF_CRM_CONTACT_BANK")


class BitrixHelperTests(unittest.TestCase):
    def setUp(self):
        _clear_runtime_caches()
        self.app = create_app()
        self.ctx = self.app.test_request_context("/")
        self.ctx.push()
        session["domain"] = "example.bitrix24.de"
        session["token"] = "expired-token"
        session["refresh_token"] = "refresh-1"

    def tearDown(self):
        self.ctx.pop()

    def test_b24_call_raw_refreshes_expired_token_once(self):
        expired_response = Mock(status_code=401, text="expired")
        expired_response.json.return_value = {
            "error": "expired_token",
            "error_description": "The access token provided has expired.",
        }

        ok_response = Mock(status_code=200, text='{"result": []}')
        ok_response.json.return_value = {"result": []}

        with patch("app.services.bitrix_helper._session.post", side_effect=[expired_response, ok_response]) as mocked_post, \
             patch("app.services.token_manager.refresh_access_token", return_value={
                 "access_token": "fresh-token",
                 "refresh_token": "refresh-2",
                 "expires_in": 3600,
             }):
            data = b24_call_raw("example.bitrix24.de", "expired-token", "crm.deal.userfield.list")

        self.assertEqual(data, {"result": []})
        self.assertEqual(session["token"], "fresh-token")
        self.assertEqual(session["refresh_token"], "refresh-2")
        self.assertIn(("auth", "fresh-token"), mocked_post.call_args_list[1].kwargs["data"])


class RouteTests(unittest.TestCase):
    def setUp(self):
        _clear_runtime_caches()
        self.app = create_app()
        self.client = self.app.test_client()
        self.app_context = self.app.app_context()
        self.app_context.push()

    def tearDown(self):
        self.app_context.pop()

    def _set_session_auth(self):
        with self.client.session_transaction() as sess:
            sess["token"] = "session-token"
            sess["domain"] = "example.bitrix24.de"
            sess["member_id"] = "member-1"

    def test_index_keeps_auth_context_in_rendered_forms(self):
        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes._load_categories", return_value=[{"ID": "0", "NAME": "Sales"}]):
            response = self.client.get("/?auth[access_token]=token123&auth[domain]=example.bitrix24.de&auth[member_id]=member-1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('value="token123"', body)
        self.assertIn("Bitte Pipeline wählen", body)

    def test_index_renders_in_english_when_portal_language_is_english(self):
        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes._load_categories", return_value=[{"ID": "0", "NAME": "Sales"}]):
            response = self.client.get(
                "/?auth[access_token]=token123&auth[domain]=example.bitrix24.de&auth[member_id]=member-1&auth[lang]=en"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Export Center", body)
        self.assertIn("Please select a pipeline", body)
        self.assertIn('name="auth[lang]" value="en"', body)

    def test_settings_renders_contact_field_options(self):
        self._set_session_auth()

        deal_fields = [{
            "FIELD_NAME": "UF_CRM_DEAL_MANDATE",
            "USER_TYPE_ID": "string",
            "EDIT_FORM_LABEL": "Mandat",
        }]
        contact_fields = [{
            "FIELD_NAME": "UF_CRM_CONTACT_IBAN",
            "USER_TYPE_ID": "string",
            "EDIT_FORM_LABEL": "IBAN",
        }]

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=deal_fields), \
             patch("app.routes.routes.list_contact_userfields", return_value=contact_fields):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("UF_CRM_DEAL_MANDATE", body)
        self.assertIn("UF_CRM_CONTACT_IBAN", body)

    def test_settings_renders_in_english_when_portal_language_is_english(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings?auth[lang]=en")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("SEPA Settings", body)
        self.assertIn("Upgrade to Pro", body)
        self.assertIn("Creditor details", body)

    def test_settings_renders_language_switch(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings?auth[lang]=en")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('data-language-mode="auto"', body)
        self.assertIn('action="/set-language"', body)
        self.assertIn("App language", body)
        self.assertIn('name="ui_lang" value="auto"', body)
        self.assertIn('name="ui_lang" value="de"', body)
        self.assertIn('name="ui_lang" value="en"', body)

    def test_manual_language_override_keeps_ui_language(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_set") as mocked_set:
            response = self.client.post(
                "/set-language",
                data={
                    "ui_lang": "en",
                    "next": "/settings",
                    "auth[lang]": "de",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(urlparse(response.headers["Location"]).path, "/settings")
        mocked_set.assert_called_with("example.bitrix24.de", "session-token", "UI_LANGUAGE_OVERRIDE", "EN")

        with self.client.session_transaction() as sess:
            self.assertEqual(sess["app_lang_override"], "en")
            self.assertEqual(sess["app_lang"], "en")
            self.assertEqual(sess["portal_lang_code"], "de")

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings?auth[lang]=de")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("SEPA Settings", body)
        self.assertIn('data-language-mode="en"', body)

    def test_auto_language_mode_follows_portal_again(self):
        self._set_session_auth()
        with self.client.session_transaction() as sess:
            sess["app_lang_override"] = "en"
            sess["app_lang"] = "en"
            sess["portal_lang_code"] = "en"

        with patch("app.routes.routes.app_opt_set") as mocked_set:
            response = self.client.post(
                "/set-language",
                data={
                    "ui_lang": "auto",
                    "next": "/settings",
                    "auth[lang]": "de",
                },
            )

        self.assertEqual(response.status_code, 302)
        mocked_set.assert_called_with("example.bitrix24.de", "session-token", "UI_LANGUAGE_OVERRIDE", "AUTO")

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings?auth[lang]=de")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("SEPA Einstellungen", body)
        self.assertIn('data-language-mode="auto"', body)

    def test_export_error_uses_index_target_for_language_switch(self):
        self._set_session_auth()

        with patch("app.routes.routes._load_categories", return_value=[{"ID": "5", "NAME": "Sales"}]), \
             patch("app.routes.routes._populate_index_listing"), \
             patch("app.routes.routes.create_user_if_not_exists"), \
             patch("app.routes.routes.get_user", return_value={"plan": "free", "exports_used": 1}), \
             patch("app.routes.routes.can_export", return_value=False):
            response = self.client.post(
                "/export",
                data={
                    "category_id": "5",
                    "stage_id": "C5:NEW",
                    "page": "2",
                    "page_size": "25",
                    "lcl_instr": "CORE",
                    "seq": "OOFF",
                    "deal_id": "123",
                },
            )

        self.assertEqual(response.status_code, 403)
        body = response.get_data(as_text=True)
        self.assertIn(
            'name="next" value="/?category_id=5&amp;stage_id=C5%3ANEW&amp;page=2&amp;page_size=25&amp;lcl_instr=CORE&amp;seq=OOFF"',
            body,
        )

    def test_set_language_rejects_post_only_next_target(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_set") as mocked_set:
            response = self.client.post(
                "/set-language",
                data={
                    "ui_lang": "en",
                    "next": "/export",
                    "auth[lang]": "de",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(urlparse(response.headers["Location"]).path, "/settings")
        mocked_set.assert_called_with("example.bitrix24.de", "session-token", "UI_LANGUAGE_OVERRIDE", "EN")

    def test_saved_language_override_is_loaded_from_bitrix_options(self):
        self._set_session_auth()

        def option_value(domain, token, key):
            if key == "UI_LANGUAGE_OVERRIDE":
                return "EN"
            return ""

        with patch("app.routes.routes.app_opt_get", side_effect=option_value), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings?auth[lang]=de")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("SEPA Settings", body)
        self.assertIn('data-language-mode="en"', body)

        with self.client.session_transaction() as sess:
            self.assertEqual(sess["app_lang_override"], "en")

    def test_settings_reads_creditor_data_from_bitrix_options(self):
        self._set_session_auth()

        option_values = {
            "CREDITOR_NAME": "Portal Creditor",
            "CREDITOR_IBAN": "PORTAL-IBAN",
            "CREDITOR_BIC": "PORTAL-BIC",
            "CREDITOR_CI": "PORTAL-CI",
        }

        with patch("app.routes.routes.app_opt_get", side_effect=lambda domain, token, key: option_values.get(key, "")), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('value="Portal Creditor"', body)
        self.assertIn('value="PORTAL-IBAN"', body)

    def test_settings_handles_missing_field_labels(self):
        self._set_session_auth()

        deal_fields = [{
            "FIELD_NAME": "UF_CRM_DEAL_NO_LABEL",
            "USER_TYPE_ID": "string",
            "EDIT_FORM_LABEL": None,
            "LIST_COLUMN_LABEL": None,
        }]

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=deal_fields), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("UF_CRM_DEAL_NO_LABEL", body)

    def test_settings_get_does_not_run_initial_scan_anymore(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.scan_field_codes") as mocked_scan, \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Auto-Erkennung wurde ausgeführt.", body)
        mocked_scan.assert_not_called()

    def test_manual_field_setup_rescans_all_when_form_fields_are_blank(self):
        self._set_session_auth()

        option_values = {
            "FIELD_DEBTOR_NAME": "UF_CRM_OLD_DEBTOR",
            "FIELD_MANDATE_ID": "UF_CRM_OLD_MANDATE_ID",
            "FIELD_MANDATE_DATE": "UF_CRM_OLD_MANDATE_DATE",
            "FIELD_CONTACT_IBAN": "UF_CRM_OLD_IBAN",
        }

        with patch("app.routes.routes.app_opt_get", side_effect=lambda domain, token, key: option_values.get(key, "")), \
             patch("app.routes.routes.app_opt_set") as mocked_set, \
             patch("app.routes.routes.scan_field_codes", return_value={
                 "DEBTOR_NAME": "UF_CRM_SEPA_DEBTOR",
                 "MANDATE_ID": "UF_CRM_SEPA_MANDATE_ID",
                 "MANDATE_DATE": "UF_CRM_SEPA_MANDATE_DATE",
                 "CONTACT_IBAN": "UF_CRM_SEPA_IBAN",
             }), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.post(
                "/debug_detect_mandate_fields",
                data={
                    "auth[access_token]": "session-token",
                    "auth[domain]": "example.bitrix24.de",
                    "auth[member_id]": "member-1",
                    "field_name": "",
                    "field_mand_id": "",
                    "field_mand_date": "",
                    "field_contact_iban": "",
                },
                follow_redirects=True,
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Auto-Erkennung wurde ausgeführt.", body)
        self.assertIn("Felder gefunden in Aufträge: SEPA Debitor-Name, SEPA Mandats-ID, SEPA Mandatsdatum.", body)
        self.assertIn("Felder gefunden in Kontakte: SEPA IBAN.", body)
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_DEBTOR_NAME", "UF_CRM_SEPA_DEBTOR")
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_MANDATE_ID", "UF_CRM_SEPA_MANDATE_ID")
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_MANDATE_DATE", "UF_CRM_SEPA_MANDATE_DATE")
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_CONTACT_IBAN", "UF_CRM_SEPA_IBAN")

    def test_manual_field_setup_creates_missing_fields_when_only_some_are_blank(self):
        self._set_session_auth()

        option_values = {
            "FIELD_DEBTOR_NAME": "UF_CRM_FIXED_DEBTOR",
            "FIELD_MANDATE_ID": "",
            "FIELD_MANDATE_DATE": "",
            "FIELD_CONTACT_IBAN": "",
        }
        created_deal_fields = [
            {"FIELD_NAME": "UF_CRM_SEPA_MANDATE_DATE", "USER_TYPE_ID": "date", "EDIT_FORM_LABEL": "SEPA Mandatsdatum"},
        ]
        created_contact_fields = [
            {"FIELD_NAME": "UF_CRM_SEPA_IBAN", "USER_TYPE_ID": "string", "EDIT_FORM_LABEL": "SEPA IBAN"},
        ]

        def ensure_side_effect(domain, token, logical_name, userfields=None, language="de"):
            if logical_name == "CONTACT_IBAN":
                return created_contact_fields[0], True, created_contact_fields
            return created_deal_fields[0], True, created_deal_fields

        with patch("app.routes.routes.app_opt_get", side_effect=lambda domain, token, key: option_values.get(key, "")), \
             patch("app.routes.routes.app_opt_set") as mocked_set, \
             patch("app.routes.routes.scan_field_codes", return_value={
                 "MANDATE_ID": "UF_CRM_DETECTED_MANDATE_ID",
             }), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]), \
             patch("app.routes.routes.ensure_sepa_userfield", side_effect=ensure_side_effect):
            response = self.client.post(
                "/debug_detect_mandate_fields",
                data={
                    "auth[access_token]": "session-token",
                    "auth[domain]": "example.bitrix24.de",
                    "auth[member_id]": "member-1",
                    "field_name": "UF_CRM_FIXED_DEBTOR",
                    "field_mand_id": "",
                    "field_mand_date": "",
                    "field_contact_iban": "",
                },
                follow_redirects=True,
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Felder gefunden in Aufträge: SEPA Mandats-ID.", body)
        self.assertIn("Neue Felder automatisch angelegt in Aufträge: SEPA Mandatsdatum.", body)
        self.assertIn("Neue Felder automatisch angelegt in Kontakte: SEPA IBAN.", body)
        self.assertFalse(any(call.args[2] == "FIELD_DEBTOR_NAME" for call in mocked_set.call_args_list))
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_MANDATE_ID", "UF_CRM_DETECTED_MANDATE_ID")
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_MANDATE_DATE", "UF_CRM_SEPA_MANDATE_DATE")
        mocked_set.assert_any_call("example.bitrix24.de", "session-token", "FIELD_CONTACT_IBAN", "UF_CRM_SEPA_IBAN")

    def test_install_redirect_preserves_auth_query_and_session(self):
        with patch("app.routes.routes.app_opt_set"):
            response = self.client.get(
                "/install?AUTH_ID=token-install&DOMAIN=example.bitrix24.de&member_id=member-1&REFRESH_ID=refresh-1&expires=3600&lang=en"
            )

        self.assertEqual(response.status_code, 302)
        parsed = urlparse(response.headers["Location"])
        query = parse_qs(parsed.query)
        self.assertEqual(query["auth[access_token]"][0], "token-install")
        self.assertEqual(query["auth[domain]"][0], "example.bitrix24.de")
        self.assertEqual(query["auth[member_id]"][0], "member-1")
        self.assertEqual(query["auth[lang]"][0], "en")

        with self.client.session_transaction() as sess:
            self.assertEqual(sess["token"], "token-install")
            self.assertEqual(sess["refresh_token"], "refresh-1")
            self.assertEqual(sess["app_lang"], "en")

    def test_settings_accepts_non_standard_creditor_data(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_set") as mocked_set, \
             patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.post(
                "/settings",
                data={
                    "creditor_name": "Meine Firma",
                    "creditor_iban": "nicht-normgerecht 123",
                    "creditor_bic": "sonder format",
                    "creditor_ci": "ci frei format",
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Einstellungen gespeichert.", body)
        self.assertNotIn("ungültig", body)
        self.assertGreaterEqual(mocked_set.call_count, 4)

    def test_export_uses_creditor_data_from_bitrix_options(self):
        self._set_session_auth()

        option_values = {
            "CREDITOR_NAME": "Portal Creditor",
            "CREDITOR_IBAN": "PORTAL-IBAN",
            "CREDITOR_BIC": "PORTAL-BIC",
            "CREDITOR_CI": "PORTAL-CI",
        }

        with patch("app.routes.routes.app_opt_get", side_effect=lambda domain, token, key: option_values.get(key, "")), \
             patch("app.routes.routes._load_categories", return_value=[{"ID": "0", "NAME": "Sales"}]):
            response = self.client.post("/export", data={"category_id": "0"})

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Bitte mindestens einen Deal auswählen.", body)
        self.assertNotIn("Bitte Gläubiger-Name", body)

    def test_settings_reports_portal_save_failure_without_local_fallback(self):
        self._set_session_auth()

        with patch("app.routes.routes.app_opt_set", side_effect=RuntimeError("portal unavailable")), \
             patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.post(
                "/settings",
                data={
                    "creditor_name": "Meine Firma",
                    "creditor_iban": "LOCAL-IBAN",
                    "creditor_bic": "LOCAL-BIC",
                    "creditor_ci": "LOCAL-CI",
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Einstellungen gespeichert.", body)
        self.assertIn("Bitrix24 gespeichert", body)

    def test_checkout_success_uses_english_copy_when_lang_is_english(self):
        response = self.client.get("/success?app_lang=en")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Subscription activated", body)
        self.assertIn("The Pro plan is now active.", body)

    def test_debug_detect_mandate_fields_falls_back_to_auth_bootstrap_when_token_is_expired(self):
        self._set_session_auth()

        with patch("app.routes.routes.scan_field_codes", side_effect=RuntimeError("The access token provided has expired.")):
            response = self.client.get("/debug_detect_mandate_fields")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bitrix24 Verbindung", body)


if __name__ == "__main__":
    unittest.main()
