import os


def get_oauth_credentials() -> tuple[str, str]:
    client_id = os.getenv("BITRIX_CLIENT_ID")
    client_secret = os.getenv("BITRIX_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "BITRIX_CLIENT_ID oder BITRIX_CLIENT_SECRET fehlt "
            "(Environment Variables nicht gesetzt)."
        )

    return client_id, client_secret
