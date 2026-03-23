import os

from flask import Flask

from app.routes.routes import main_bp


def create_app():
    app = Flask(__name__, template_folder="../templates")

    is_prod = os.getenv("B24_ENV", "").upper() == "PROD"
    secret_key = os.getenv("FLASK_SECRET_KEY")
    if not secret_key and is_prod:
        raise RuntimeError("FLASK_SECRET_KEY fehlt im PROD-Betrieb.")

    app.secret_key = secret_key or "local-dev-sepa-app-secret"
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="None" if is_prod else "Lax",
        SESSION_COOKIE_SECURE=is_prod,
        TEMPLATES_AUTO_RELOAD=not is_prod,
    )
    app.register_blueprint(main_bp)
    return app
