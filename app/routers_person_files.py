import os
import re
import secrets
from pathlib import Path
from datetime import datetime
from typing import Optional, Any

from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse

from .db import db
from . import auth
from .permissions import require_permission


router = APIRouter(tags=["person-files"])


def oid(v: Any):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


def _norm(v: Any) -> Any:
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, list):
        return [_norm(x) for x in v]
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    return v


def _safe_filename(name: str) -> str:
    name = os.path.basename(name or "").strip() or "file"
    name = name.replace("\x00", "")
    # чистим плохие символы
    name = re.sub(r"[^\w\-.() ]+", "_", name, flags=re.U).strip()
    name = re.sub(r"\s+", " ", name).strip()
    return name[:160] or "file"


def _uploads_root() -> Path:
    backend_root = Path(__file__).resolve().parents[1]
    return backend_root / "static" / "uploads" / "persons"


@router.get("/api/person/{person_id}/files")
async def list_person_files(person_id: str, user=Depends(auth.get_current_user)):
    require_permission(user, "person_files.view")
    tid = oid(user["tenantId"])
    pid = oid(person_id)

    cur = (
        db["files"]
        .find({"tenantId": tid, "personId": pid})
        .sort([("uploadedAt", -1)])
        .limit(500)
    )
    items = await cur.to_list(length=500)
    return {"items": [_norm(x) for x in items]}


@router.post("/api/person/{person_id}/files")
async def upload_person_file(
    person_id: str,
    file: UploadFile = File(...),
    kind: Optional[str] = Form(None),         # passport / registration / face / other
    description: Optional[str] = Form(None),  # текстовое описание
    user=Depends(auth.get_current_user),
):
    require_permission(user, "person_files.upload")
    tid = oid(user["tenantId"])
    pid = oid(person_id)

    # проверим что сотрудник существует и принадлежит компании
    person = await db["person"].find_one({"_id": pid, "tenantId": tid})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    now = datetime.utcnow()

    orig = _safe_filename(file.filename or "file")
    ext = Path(orig).suffix[:12]
    stamp = now.strftime("%Y%m%d_%H%M%S")
    rnd = secrets.token_hex(4)
    stored = f"{stamp}_{rnd}{ext}"

    dir_path = _uploads_root() / str(pid)
    dir_path.mkdir(parents=True, exist_ok=True)

    abs_path = dir_path / stored
    size = 0

    with abs_path.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            size += len(chunk)

    rel_path = f"static/uploads/persons/{pid}/{stored}"

    doc = {
        "tenantId": tid,
        "personId": pid,
        "kind": (kind or "").strip() or None,
        "description": (description or "").strip() or None,
        "origName": orig,
        "storedName": stored,
        "path": rel_path,
        "contentType": file.content_type or "application/octet-stream",
        "size": size,
        "uploadedAt": now,
        "uploadedBy": oid(user["_id"]),
    }

    res = await db["files"].insert_one(doc)
    created = await db["files"].find_one({"_id": res.inserted_id})
    return _norm(created)


@router.get("/api/person-files/{file_id}/download")
async def download_person_file(file_id: str, user=Depends(auth.get_current_user)):
    require_permission(user, "person_files.view")
    tid = oid(user["tenantId"])
    fdoc = await db["files"].find_one({"_id": oid(file_id), "tenantId": tid})
    if not fdoc:
        raise HTTPException(status_code=404, detail="File not found")

    backend_root = Path(__file__).resolve().parents[1]
    abs_path = backend_root / (fdoc.get("path") or "")
    if not abs_path.exists():
        raise HTTPException(status_code=404, detail="File missing on disk")

    return FileResponse(
        path=str(abs_path),
        filename=fdoc.get("origName") or "file",
        media_type=fdoc.get("contentType") or "application/octet-stream",
    )


@router.delete("/api/person-files/{file_id}")
async def delete_person_file(file_id: str, user=Depends(auth.get_current_user)):
    require_permission(user, "person_files.delete")
    tid = oid(user["tenantId"])
    fdoc = await db["files"].find_one({"_id": oid(file_id), "tenantId": tid})
    if not fdoc:
        raise HTTPException(status_code=404, detail="File not found")

    backend_root = Path(__file__).resolve().parents[1]
    abs_path = backend_root / (fdoc.get("path") or "")

    try:
        if abs_path.exists():
            abs_path.unlink()
    except Exception:
        pass

    await db["files"].delete_one({"_id": fdoc["_id"]})
    return {"ok": True}

