import json
import time
from collections.abc import Iterable
from urllib.parse import quote

from app.services.bitrix_helper import b24_batch, b24_call

# =========================
# App-Optionen (pro Portal)
# =========================

APP_OPT_PREFIX = "SEPA_SDD_"
_OPTION_CACHE_TTL_SECONDS = 90
_CACHE_MISS = object()
_option_cache: dict[tuple[str, str], tuple[float, str | None]] = {}


def _option_cache_key(domain: str, name: str) -> tuple[str, str]:
    return (str(domain or "").strip().lower(), str(name or "").strip().upper())


def _get_cached_option(domain: str, name: str):
    cache_key = _option_cache_key(domain, name)
    entry = _option_cache.get(cache_key)
    if entry is None:
        return _CACHE_MISS

    expires_at, value = entry
    if time.time() >= expires_at:
        _option_cache.pop(cache_key, None)
        return _CACHE_MISS

    return value


def _set_cached_option(domain: str, name: str, value: str | None):
    _option_cache[_option_cache_key(domain, name)] = (
        time.time() + _OPTION_CACHE_TTL_SECONDS,
        value,
    )


def clear_app_option_cache(domain: str | None = None, *names: str):
    if domain is None:
        _option_cache.clear()
        return

    normalized_domain = str(domain or "").strip().lower()
    if not names:
        keys_to_remove = [
            cache_key
            for cache_key in _option_cache
            if cache_key[0] == normalized_domain
        ]
        for cache_key in keys_to_remove:
            _option_cache.pop(cache_key, None)
        return

    for name in names:
        _option_cache.pop(_option_cache_key(normalized_domain, name), None)


def _normalize_app_option(name: str, value) -> str | None:
    option_key = APP_OPT_PREFIX + name

    if isinstance(value, dict):
        if option_key in value:
            value = value.get(option_key)
        elif value.get("option") == option_key and "value" in value:
            # Recovery path for the old broken payload shape that stored
            # literal "option"/"value" keys instead of the actual option name.
            value = value.get("value")
        elif set(value.keys()) == {"value"}:
            value = value.get("value")
        else:
            return None

    if value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    return value or None


def _load_option_uncached(domain: str, token: str, name: str) -> str | None:
    val = b24_call(domain, token, "app.option.get", {"option": APP_OPT_PREFIX + name})
    return _normalize_app_option(name, val)


def _batch_command_for_option(name: str) -> str:
    option_name = quote(APP_OPT_PREFIX + name, safe="")
    return f"app.option.get?option={option_name}"


def app_opt_get(domain: str, token: str, name: str) -> str | None:
    cached = _get_cached_option(domain, name)
    if cached is not _CACHE_MISS:
        return cached

    try:
        value = _load_option_uncached(domain, token, name)
    except Exception:
        return None

    _set_cached_option(domain, name, value)
    return value


def app_opt_get_many(domain: str, token: str, names: Iterable[str]) -> dict[str, str | None]:
    ordered_names = []
    seen = set()
    for name in names:
        normalized_name = str(name or "").strip().upper()
        if not normalized_name or normalized_name in seen:
            continue
        seen.add(normalized_name)
        ordered_names.append(normalized_name)

    results: dict[str, str | None] = {}
    missing_names: list[str] = []

    for name in ordered_names:
        cached = _get_cached_option(domain, name)
        if cached is _CACHE_MISS:
            missing_names.append(name)
        else:
            results[name] = cached

    if not missing_names:
        return results

    try:
        batch_results = b24_batch(
            domain,
            token,
            {name: _batch_command_for_option(name) for name in missing_names},
            halt=False,
        )
    except Exception:
        batch_results = {}

    unresolved_names: list[str] = []
    for name in missing_names:
        if name not in batch_results:
            unresolved_names.append(name)
            continue

        value = _normalize_app_option(name, batch_results.get(name))
        _set_cached_option(domain, name, value)
        results[name] = value

    for name in unresolved_names:
        results[name] = app_opt_get(domain, token, name)

    return results


def app_opt_set(domain: str, token: str, name: str, value: str):
    b24_call(
        domain,
        token,
        "app.option.set",
        {"options": {APP_OPT_PREFIX + name: value if value is not None else ""}},
    )
    clear_app_option_cache(domain, name)


def load_fieldmap(domain: str, token: str) -> dict:
    raw = app_opt_get(domain, token, "FIELDMAP_JSON")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def save_fieldmap(domain: str, token: str, fmap: dict):
    app_opt_set(domain, token, "FIELDMAP_JSON", json.dumps(fmap))
