from fastapi import APIRouter, Depends, HTTPException, UploadFile
from pathlib import Path
from datetime import datetime
from bson import ObjectId

from .db import db
from . import auth
from .audit import log_user_action

router = APIRouter(tags=["project-files"])

# путь хранения - backend/static/uploads/projects/<projectId>/<filename>
UPLOAD_ROOT = Path("static/uploads/projects")


def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


def _normalize_file(d: dict) -> dict:
    return {
        "id": str(d["_id"]),
        "projectId": str(d["projectId"]),
        "filename": d["filename"],
        "size": d.get("size"),
        "contentType": d.get("contentType"),
        "uploadedBy": str(d.get("uploadedBy")) if d.get("uploadedBy") else None,
        "uploadedAt": d.get("uploadedAt"),
    }

def _file_meta(file_doc: dict, project: dict | None = None) -> dict:
    return {
        "fileId": str(file_doc.get("_id")) if file_doc.get("_id") else None,
        "projectId": str(file_doc.get("projectId")) if file_doc.get("projectId") else None,
        "projectName": (project or {}).get("name") if project else None,
        "filename": file_doc.get("filename"),
        "size": file_doc.get("size"),
        "contentType": file_doc.get("contentType"),
        "uploadedBy": str(file_doc.get("uploadedBy")) if file_doc.get("uploadedBy") else None,
        "uploadedAt": file_doc.get("uploadedAt"),
    }


@router.get("/api/projects/{pid}/files")
async def list_files(pid: str, user=Depends(auth.get_current_user)):
    proj = await db.projects.find_one({"_id": oid(pid), "tenantId": oid(user["tenantId"])})
    if not proj:
        raise HTTPException(404, "Проект не найден")

    cur = db.files.find({"tenantId": oid(user["tenantId"]), "projectId": oid(pid)}).sort("uploadedAt", -1)
    items = []
    async for d in cur:
        items.append(_normalize_file(d))
    return {"items": items}


@router.post("/api/projects/{pid}/files")
async def upload_file(pid: str, file: UploadFile, user=Depends(auth.get_current_user)):
    proj = await db.projects.find_one({"_id": oid(pid), "tenantId": oid(user["tenantId"])})
    if not proj:
        raise HTTPException(404, "Проект не найден")

    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    folder = UPLOAD_ROOT / str(pid)
    folder.mkdir(parents=True, exist_ok=True)

    target = folder / file.filename
    # пишем чанками
    with target.open("wb") as fout:
        while chunk := await file.read(1024 * 1024):
            fout.write(chunk)

    doc = {
        "tenantId": oid(user["tenantId"]),
        "projectId": oid(pid),
        "filename": file.filename,
        "path": str(target),
        "size": target.stat().st_size,
        "contentType": file.content_type,
        "uploadedBy": oid(user["_id"]),
        "uploadedAt": datetime.utcnow(),
    }
    res = await db.files.insert_one(doc)
    saved_doc = {**doc, "_id": res.inserted_id}

    # лог
    try:
        await log_user_action(
            db,
            user,
            action="project.file.upload",
            entity="project.file",
            entity_id=str(res.inserted_id),
            message=f'Загружен файл «{file.filename}» в проект «{proj.get("name")}».',
            meta=_file_meta(saved_doc, proj),
        )
    except Exception:
        pass

    return {"ok": True, "file": _normalize_file(saved_doc)}


@router.delete("/api/files/{fid}")
async def delete_file(fid: str, user=Depends(auth.get_current_user)):
    f = await db.files.find_one({"_id": oid(fid), "tenantId": oid(user["tenantId"])})
    if not f:
        raise HTTPException(404, "Файл не найден")

    # удаляем с диска (если есть)
    try:
        Path(f["path"]).unlink(missing_ok=True)
    except Exception:
        pass

    await db.files.delete_one({"_id": oid(fid)})

    # лог
    try:
        await log_user_action(
            db,
            user,
            action="project.file.delete",
            entity="project.file",
            entity_id=str(fid),
            message=f'Удалён файл «{f.get("filename")}» из проекта.',
            meta=_file_meta(f),
        )
    except Exception:
        pass

    return {"ok": True}
