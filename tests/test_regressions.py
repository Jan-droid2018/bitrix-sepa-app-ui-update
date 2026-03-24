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
from app.config.app_options import app_opt_get, app_opt_set
from app.domain.userfields import list_deals_page
from app.services.bitrix_helper import b24_call_raw
from app.services.export import PAIN_008_NS, build_pain008_xml


class AppOptionsTests(unittest.TestCase):
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


class BitrixHelperTests(unittest.TestCase):
    def setUp(self):
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
        with patch("app.routes.routes._load_categories", return_value=[{"ID": "0", "NAME": "Sales"}]):
            response = self.client.get("/?auth[access_token]=token123&auth[domain]=example.bitrix24.de&auth[member_id]=member-1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('value="token123"', body)
        self.assertIn("Bitte Pipeline wählen", body)

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

    def test_install_redirect_preserves_auth_query_and_session(self):
        with patch("app.routes.routes.app_opt_set"):
            response = self.client.get(
                "/install?AUTH_ID=token-install&DOMAIN=example.bitrix24.de&member_id=member-1&REFRESH_ID=refresh-1&expires=3600"
            )

        self.assertEqual(response.status_code, 302)
        parsed = urlparse(response.headers["Location"])
        query = parse_qs(parsed.query)
        self.assertEqual(query["auth[access_token]"][0], "token-install")
        self.assertEqual(query["auth[domain]"][0], "example.bitrix24.de")
        self.assertEqual(query["auth[member_id]"][0], "member-1")

        with self.client.session_transaction() as sess:
            self.assertEqual(sess["token"], "token-install")
            self.assertEqual(sess["refresh_token"], "refresh-1")

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

    def test_debug_detect_mandate_fields_falls_back_to_auth_bootstrap_when_token_is_expired(self):
        self._set_session_auth()

        with patch("app.routes.routes.resolve_field_codes", side_effect=RuntimeError("The access token provided has expired.")):
            response = self.client.get("/debug_detect_mandate_fields")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bitrix24 Verbindung", body)


if __name__ == "__main__":
    unittest.main()
