import os
import re

from app.services.bitrix_helper import b24_call, b24_list_all

# =========================
# Kategorien (Pipelines)
# =========================


def list_categories(domain: str, access_token: str) -> list[dict]:
    """
    Laedt alle Deal-Pipelines.
    Optional koennen relevante Namen per ENV gefiltert werden:
    B24_CATEGORY_NAME_KEYWORDS="rechnung,rechnungen,sepa,export"
    """
    cats = b24_list_all(domain, access_token, "crm.dealcategory.list") or []

    default_raw = None
    try:
        default_raw = b24_call(domain, access_token, "crm.dealcategory.default.get")
    except Exception:
        pass

    def _to_int(value):
        try:
            return int(value)
        except Exception:
            return None

    default_id = None
    default_name = None
    if isinstance(default_raw, dict):
        default_id = _to_int(default_raw.get("ID"))
        default_name = default_raw.get("NAME") or default_raw.get("name")
    else:
        default_id = _to_int(default_raw)

    if default_id is None:
        default_id = 0

    if not default_name:
        try:
            default_category = b24_call(domain, access_token, "crm.dealcategory.get", {"id": default_id})
            if isinstance(default_category, dict):
                default_name = default_category.get("NAME") or default_category.get("name")
        except Exception:
            pass

    if not default_name:
        default_name = "Sales"

    out = []
    seen_ids = set()
    for category in cats:
        cid = _to_int(category.get("ID") or category.get("id"))
        if cid is None:
            continue

        name = category.get("NAME") or category.get("name") or str(cid)
        out.append({"ID": str(cid), "NAME": name})
        seen_ids.add(cid)

    if default_id not in seen_ids:
        out.insert(0, {"ID": str(default_id), "NAME": default_name})

    kw_env = os.getenv("B24_CATEGORY_NAME_KEYWORDS", "")
    keywords = [item.strip().lower() for item in kw_env.split(",") if item.strip()]

    def match_any(name: str) -> bool:
        normalized = (name or "").lower()
        return any(keyword in normalized for keyword in keywords) if keywords else True

    filtered = [category for category in out if match_any(category["NAME"])]
    final = filtered if filtered else out
    final.sort(
        key=lambda item: (
            0 if item["ID"].isdigit() and int(item["ID"]) == default_id else 1,
            int(item["ID"]) if item["ID"].isdigit() else 99_999,
        )
    )
    return final


# =========================
# Robust Category-ID Parser
# =========================


def _parse_category_id(val) -> int | None:
    if val in (None, ""):
        return None
    if isinstance(val, int):
        return val
    text = str(val).strip()
    try:
        return int(text)
    except Exception:
        pass
    match = re.search(r"[\"']?ID[\"']?\s*[:=]\s*([0-9]+)", text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            pass
    match = re.search(r"\b([0-9]+)\b", text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            pass
    return None
