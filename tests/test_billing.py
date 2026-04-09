import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import db
from app import create_app
from app.config.app_options import clear_app_option_cache
from app.domain.categories import clear_category_cache
from app.domain.userfields import clear_userfield_cache


def _clear_runtime_caches():
    clear_app_option_cache()
    clear_category_cache()
    clear_userfield_cache()


class BillingDbTests(unittest.TestCase):
    def setUp(self):
        _clear_runtime_caches()
        self.test_db_path = Path(__file__).resolve().parents[1] / f"billing_test_{uuid.uuid4().hex}.db"
        self.original_db_path = db.DB_PATH
        db.DB_PATH = self.test_db_path
        db._init_db()

    def tearDown(self):
        db.DB_PATH = self.original_db_path
        if self.test_db_path.exists():
            try:
                self.test_db_path.unlink()
            except PermissionError:
                pass

    def test_free_plan_allows_one_export_per_month(self):
        user = db.create_user_if_not_exists("member-free")
        self.assertTrue(db.can_export(user))

        db.increase_export("member-free")
        user = db.get_user("member-free")

        self.assertEqual(user["plan"], "free")
        self.assertEqual(user["exports_used"], 1)
        self.assertFalse(db.can_export(user))

    def test_month_change_resets_export_counter(self):
        db.create_user_if_not_exists("member-reset")
        db.increase_export("member-reset")

        with db._connect() as conn:
            conn.execute(
                "UPDATE users SET last_reset = ?, exports_used = 1 WHERE member_id = ?",
                ("2026-02-01", "member-reset"),
            )
            conn.commit()

        user = db.get_user("member-reset")
        self.assertEqual(user["exports_used"], 0)
        self.assertTrue(db.can_export(user))

    def test_pro_plan_is_unlimited(self):
        db.set_plan("member-pro", "pro")
        db.increase_export("member-pro")
        db.increase_export("member-pro")

        user = db.get_user("member-pro")
        self.assertEqual(user["plan"], "pro")
        self.assertEqual(user["exports_used"], 0)
        self.assertTrue(db.can_export(user))


class BillingRouteTests(unittest.TestCase):
    def setUp(self):
        _clear_runtime_caches()
        os.environ.setdefault("BITRIX_CLIENT_ID", "test-client")
        os.environ.setdefault("BITRIX_CLIENT_SECRET", "test-secret")

        self.test_db_path = Path(__file__).resolve().parents[1] / f"billing_test_{uuid.uuid4().hex}.db"
        self.original_db_path = db.DB_PATH
        db.DB_PATH = self.test_db_path
        db._init_db()

        self.app = create_app()
        self.client = self.app.test_client()
        self.app_context = self.app.app_context()
        self.app_context.push()

    def tearDown(self):
        self.app_context.pop()
        db.DB_PATH = self.original_db_path
        if self.test_db_path.exists():
            try:
                self.test_db_path.unlink()
            except PermissionError:
                pass

    def _set_session_auth(self):
        with self.client.session_transaction() as sess:
            sess["token"] = "session-token"
            sess["domain"] = "example.bitrix24.de"
            sess["member_id"] = "member-1"

    def test_export_blocks_when_free_limit_is_reached(self):
        self._set_session_auth()
        db.create_user_if_not_exists("member-1")
        db.increase_export("member-1")

        option_values = {
            "CREDITOR_NAME": "Portal Creditor",
            "CREDITOR_IBAN": "PORTAL-IBAN",
            "CREDITOR_BIC": "PORTAL-BIC",
            "CREDITOR_CI": "PORTAL-CI",
        }

        with patch("app.routes.routes.app_opt_get", side_effect=lambda domain, token, key: option_values.get(key, "")), \
             patch("app.routes.routes._load_categories", return_value=[{"ID": "0", "NAME": "Sales"}]):
            response = self.client.post(
                "/export",
                data={
                    "auth[member_id]": "member-1",
                    "category_id": "0",
                },
            )

        self.assertEqual(response.status_code, 403)
        self.assertIn("Free Plan erreicht", response.get_data(as_text=True))

    def test_settings_shows_upgrade_button_for_free_plan(self):
        self._set_session_auth()
        db.create_user_if_not_exists("member-1")

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Upgrade auf Pro", body)
        self.assertIn("Plan &amp; Upgrade", body)

    def test_settings_shows_pro_status_when_subscription_is_active(self):
        self._set_session_auth()
        db.set_plan("member-1", "pro")

        with patch("app.routes.routes.app_opt_get", return_value=""), \
             patch("app.routes.routes.get_deal_userfields", return_value=[]), \
             patch("app.routes.routes.list_contact_userfields", return_value=[]):
            response = self.client.get("/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Pro aktiv", body)
        self.assertNotIn('id="upgrade-button"', body)

    def test_create_checkout_session_includes_member_id_metadata(self):
        fake_session = Mock(id="cs_123", url="https://checkout.stripe.test/session")
        fake_checkout = Mock()
        fake_checkout.Session.create.return_value = fake_session
        fake_stripe = Mock(checkout=fake_checkout)

        with patch("app.routes.routes._get_stripe_api", return_value=fake_stripe), \
             patch.dict(os.environ, {"STRIPE_PRICE_ID": "price_123"}, clear=False):
            response = self.client.post(
                "/create-checkout-session",
                data={"auth[member_id]": "member-1"},
            )

        self.assertEqual(response.status_code, 200)
        fake_checkout.Session.create.assert_called_once()
        kwargs = fake_checkout.Session.create.call_args.kwargs
        self.assertEqual(kwargs["mode"], "subscription")
        self.assertEqual(kwargs["metadata"]["member_id"], "member-1")
        self.assertEqual(kwargs["subscription_data"]["metadata"]["member_id"], "member-1")

    def test_create_checkout_session_uses_manual_ui_language_for_return_page(self):
        fake_session = Mock(id="cs_123", url="https://checkout.stripe.test/session")
        fake_checkout = Mock()
        fake_checkout.Session.create.return_value = fake_session
        fake_stripe = Mock(checkout=fake_checkout)

        with self.client.session_transaction() as sess:
            sess["token"] = "session-token"
            sess["domain"] = "example.bitrix24.de"
            sess["member_id"] = "member-1"
            sess["app_lang_override"] = "en"
            sess["app_lang"] = "en"
            sess["portal_lang_code"] = "de"

        with patch("app.routes.routes._get_stripe_api", return_value=fake_stripe), \
             patch.dict(os.environ, {"STRIPE_PRICE_ID": "price_123"}, clear=False):
            response = self.client.post(
                "/create-checkout-session",
                data={"auth[member_id]": "member-1", "auth[lang]": "de"},
            )

        self.assertEqual(response.status_code, 200)
        kwargs = fake_checkout.Session.create.call_args.kwargs
        self.assertIn("app_lang=en", kwargs["success_url"])
        self.assertIn("app_lang=en", kwargs["cancel_url"])

    def test_webhook_checkout_completed_sets_pro_plan(self):
        event = {
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "metadata": {"member_id": "member-1"},
                    "customer": "cus_123",
                    "subscription": "sub_123",
                }
            },
        }

        fake_webhook = Mock()
        fake_webhook.construct_event.return_value = event
        fake_stripe = Mock(Webhook=fake_webhook)

        with patch("app.routes.routes._import_stripe", return_value=fake_stripe), \
             patch.dict(os.environ, {"STRIPE_WEBHOOK_SECRET": "whsec_test"}, clear=False):
            response = self.client.post(
                "/stripe/webhook",
                data=b"{}",
                headers={"Stripe-Signature": "sig_test"},
            )

        self.assertEqual(response.status_code, 200)
        user = db.get_user("member-1")
        self.assertEqual(user["plan"], "pro")
        self.assertEqual(user["stripe_customer_id"], "cus_123")
        self.assertEqual(user["stripe_subscription_id"], "sub_123")


if __name__ == "__main__":
    unittest.main()
