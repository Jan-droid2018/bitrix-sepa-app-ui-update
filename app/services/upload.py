from .bitrix_helper import (_session, b24_call)

# =========================
# Datei-Upload & Stage-Helfer (mit Debug)
# =========================

SEPA_FOLDER_NAMES = ("SEPA-Export-Dateien", "SEPA-Export-Datein")


def _debug_log(debug_lines: list[str], msg: str):
    try:
        debug_lines.append(msg)
    except Exception:
        pass
    try:
        print("[SEPA-DEBUG]", msg, flush=True)
    except Exception:
        pass


def upload_bytes_to_folder_verbose(domain: str, token: str, folder_id: int | str, filename: str, content: bytes,
                                   debug_lines: list[str] | None = None) -> dict:
    """
    Versucht Upload via disk.folder.uploadfile (requests Multipart); wenn API stattdessen einen
    Pre-Signed-Upload-Link zurueckgibt (uploadUrl + field), wird automatisch ein zweiter POST
    an diese URL durchgefuehrt. Fallback: disk.storage.uploadfile. Liefert Debug-Rohdaten zurueck.
    Rueckgabe:
      {
        "file": {...} | None,
        "raw_folder_upload": {...} | None,
        "raw_upload_step2": {...} | None,
        "raw_storage_upload": {...} | None,
        "endpoint_used": "disk.folder.uploadfile" | "uploadUrl" | "disk.storage.uploadfile" | None
      }
    """
    import json as _json

    debug_lines = debug_lines or []
    result = {"file": None, "raw_folder_upload": None, "raw_upload_step2": None, "raw_storage_upload": None,
              "endpoint_used": None}

    url1 = f"https://{domain}/rest/disk.folder.uploadfile"
    files = {"fileContent": (filename, content, "application/xml")}
    data = {"id": str(folder_id), "auth": token}
    _debug_log(debug_lines, f"POST {url1} id={folder_id} filename={filename} size={len(content)}")
    r1 = _session.post(url1, data=data, files=files, timeout=120)
    try:
        j1 = r1.json()
    except Exception:
        j1 = {"_parse_error": True, "_raw": r1.text[:1000]}
    _debug_log(debug_lines, f"HTTP {r1.status_code} headers={dict(list(r1.headers.items())[:6])}")
    _debug_log(debug_lines, "resp1: " + _json.dumps(j1)[:1000])
    result["raw_folder_upload"] = j1

    if r1.status_code < 400 and isinstance(j1, dict) and not j1.get("error"):
        file_obj = (j1.get("result") or {})
        fid = file_obj.get("ID") or file_obj.get("id")
        upload_info = None
        if not fid:
            result_info = j1.get("result") or {}
            if isinstance(result_info, dict) and result_info.get("uploadUrl") and result_info.get("field"):
                upload_info = {"url": result_info.get("uploadUrl"), "field": result_info.get("field")}
        if fid:
            result["file"] = file_obj
            result["endpoint_used"] = "disk.folder.uploadfile"
            return result
        if upload_info:
            up_url = upload_info["url"]
            field_name = str(upload_info["field"])
            _debug_log(debug_lines, f"POST {up_url} (second step) with field={field_name}")
            r1b = _session.post(up_url, files={field_name: (filename, content, "application/xml")}, timeout=180)
            try:
                j1b = r1b.json()
            except Exception:
                j1b = {"_parse_error": True, "_raw": r1b.text[:1000]}
            _debug_log(debug_lines, f"HTTP {r1b.status_code} (step2) headers={dict(list(r1b.headers.items())[:6])}")
            _debug_log(debug_lines, "resp1b: " + _json.dumps(j1b)[:1000])
            result["raw_upload_step2"] = j1b

            file_obj = None
            if isinstance(j1b, dict):
                candidate = j1b.get("result") or j1b.get("FILE") or j1b.get("file") or j1b.get("data") or {}
                if isinstance(candidate, dict):
                    file_obj = candidate.get("file") if "file" in candidate else candidate
            fid = None
            if isinstance(file_obj, dict):
                fid = file_obj.get("ID") or file_obj.get("id")
            if fid:
                result["file"] = file_obj
                result["endpoint_used"] = "uploadUrl"
                return result

    storage_id = None
    try:
        folder = b24_call(domain, token, "disk.folder.get", {"id": str(folder_id)}) or {}
        storage_id = folder.get("STORAGE_ID") or folder.get("storageId") or (folder.get("STORAGE") or {}).get("ID")
    except Exception as exc:
        _debug_log(debug_lines, f"disk.folder.get failed: {exc}")
    if storage_id:
        url2 = f"https://{domain}/rest/disk.storage.uploadfile"
        files2 = {"fileContent": (filename, content, "application/xml")}
        data2 = {"id": str(storage_id), "auth": token}
        _debug_log(debug_lines, f"POST {url2} id={storage_id} (fallback)")
        r2 = _session.post(url2, data=data2, files=files2, timeout=120)
        try:
            j2 = r2.json()
        except Exception:
            j2 = {"_parse_error": True, "_raw": r2.text[:1000]}
        _debug_log(debug_lines, f"HTTP {r2.status_code} headers={dict(list(r2.headers.items())[:6])}")
        _debug_log(debug_lines, "resp2: " + _json.dumps(j2)[:1000])
        result["raw_storage_upload"] = j2
        if r2.status_code < 400 and isinstance(j2, dict) and not j2.get("error"):
            file_obj = (j2.get("result") or {})
            fid = file_obj.get("ID") or file_obj.get("id")
            if fid:
                result["file"] = file_obj
                result["endpoint_used"] = "disk.storage.uploadfile"
                return result

    return result


def ensure_company_sepa_folder(domain: str, token: str) -> dict:
    """
    Sucht das Unternehmens-Drive (ENTITY_TYPE=common).
    Legt darunter den Ordner 'SEPA-Export-Dateien' an (falls nicht vorhanden)
    und akzeptiert den Legacy-Namen 'SEPA-Export-Datein' weiterhin.
    """
    storages = b24_call(domain, token, "disk.storage.getlist") or []
    common = None
    for storage in storages:
        if storage.get("ENTITY_TYPE") == "common":
            common = storage
            break
    if not common:
        raise RuntimeError("Kein Unternehmens-Drive gefunden (disk.storage.getlist).")

    root_id = common.get("ROOT_OBJECT_ID") or common.get("rootObjectId")
    if not root_id:
        raise RuntimeError("ROOT_OBJECT_ID im Unternehmens-Drive fehlt.")

    kids = b24_call(domain, token, "disk.folder.getchildren", {"id": root_id}) or []
    valid_names = {name.lower() for name in SEPA_FOLDER_NAMES}
    for child in kids:
        folder_name = (child.get("NAME") or "").strip().lower()
        if child.get("TYPE") == "folder" and folder_name in valid_names:
            return child

    sepa_folder = b24_call(domain, token, "disk.folder.addsubfolder", {
        "id": root_id,
        "data": {"NAME": SEPA_FOLDER_NAMES[0]}
    })
    if not sepa_folder:
        raise RuntimeError("SEPA-Export-Dateien-Ordner konnte nicht erstellt werden.")
    return sepa_folder


def find_sepa_stage_id(domain: str, token: str, category_id: int | None) -> str:
    """
    Liefert die STATUS_ID einer Stage in der Kategorie, die 'SEPA' heisst (case-insensitive).
    Fallback: erste Stage der Kategorie (oder 'NEW' wenn Standardkategorie).
    """
    if category_id is None:
        try:
            default_cat = b24_call(domain, token, "crm.dealcategory.default.get")
            if isinstance(default_cat, dict):
                category_id = int(default_cat.get("ID") or 0)
            else:
                category_id = int(default_cat or 0)
        except Exception:
            category_id = 0
    stages = b24_call(domain, token, "crm.dealcategory.stage.list", {"id": int(category_id)}) or []
    wanted = None
    for stage in stages:
        name = (stage.get("NAME") or "").lower()
        sid = (stage.get("STATUS_ID") or "").upper()
        if "sepa" in name or "SEPA" in sid:
            wanted = stage
            break
    if not wanted and stages:
        wanted = stages[0]
    if wanted and wanted.get("STATUS_ID"):
        return wanted["STATUS_ID"]
    if isinstance(category_id, int) and category_id > 0:
        return f"C{category_id}:NEW"
    return "NEW"
