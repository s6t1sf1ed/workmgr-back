from fastapi import APIRouter, Depends, Body, HTTPException, Query
from typing import Any, Dict, Optional, List
from datetime import datetime
from bson import ObjectId

from . import auth
from .db import db

router = APIRouter()

# ---------------------- helpers ----------------------

def _oid(x):
    if isinstance(x, ObjectId):
        return x
    try:
        return ObjectId(x)
    except Exception:
        return x

def _norm(v: Any) -> Any:
    """Преобразовать ObjectId в str, рекурсивно."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, list):
        return [_norm(x) for x in v]
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    return v

def _num(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)

# =====================================================
#                      СЕКЦИИ
# =====================================================

@router.get("/api/projects/{project_id}/spec/sections")
async def spec_sections_list(
    project_id: str,
    deleted: Optional[int] = Query(None),
    user=Depends(auth.get_current_user),
):
    q = {
        "tenantId": _oid(user["tenantId"]),
        "projectId": _oid(project_id),
    }
    if deleted is not None:
        q["deleted"] = bool(int(deleted))
    items = (
        await db["spec_sections"]
        .find(q)
        .sort([("order", 1), ("createdAt", 1)])
        .to_list(100000)
    )
    return {"items": _norm(items)}

@router.post("/api/projects/{project_id}/spec/sections")
async def spec_sections_create(
    project_id: str,
    payload: dict = Body(...),
    user=Depends(auth.get_current_user),
):
    now = datetime.utcnow()
    title = (payload.get("title") or "").strip() or "Раздел"

    # order = max(order)+1
    last = (
        await db["spec_sections"]
        .find({"tenantId": _oid(user["tenantId"]), "projectId": _oid(project_id)})
        .sort([("order", -1)])
        .limit(1)
        .to_list(1)
    )
    next_order = int((last[0]["order"] if last else 0) or 0) + 1

    doc = {
        "tenantId": _oid(user["tenantId"]),
        "projectId": _oid(project_id),
        "title": title,
        "order": next_order,
        "deleted": False,
        "createdAt": now,
        "updatedAt": now,
    }
    res = await db["spec_sections"].insert_one(doc)
    created = await db["spec_sections"].find_one({"_id": res.inserted_id})
    return _norm(created)

@router.patch("/api/spec/sections/{section_id}")
async def spec_sections_update(
    section_id: str,
    payload: dict = Body(...),
    user=Depends(auth.get_current_user),
):
    now = datetime.utcnow()
    sec = await db["spec_sections"].find_one(
        {"_id": _oid(section_id), "tenantId": _oid(user["tenantId"])}
    )
    if not sec:
        raise HTTPException(404, "Section not found")

    updates: Dict[str, Any] = {}
    if "title" in payload:
        updates["title"] = (payload.get("title") or "").strip()
    if "order" in payload:
        try:
            updates["order"] = int(payload.get("order") or 0)
        except Exception:
            pass
    if "deleted" in payload:
        updates["deleted"] = bool(payload["deleted"])

    if not updates:
        return _norm(sec)

    updates["updatedAt"] = now
    await db["spec_sections"].update_one({"_id": sec["_id"]}, {"$set": updates})
    after = await db["spec_sections"].find_one({"_id": sec["_id"]})
    return _norm(after)

@router.delete("/api/spec/sections/{section_id}")
async def spec_sections_delete_forever(
    section_id: str,
    user=Depends(auth.get_current_user),
):
    sec = await db["spec_sections"].find_one(
        {"_id": _oid(section_id), "tenantId": _oid(user["tenantId"])}
    )
    if not sec:
        raise HTTPException(404, "Section not found")

    # удалим все позиции этой секции безвозвратно
    await db["spec_items"].delete_many(
        {"tenantId": _oid(user["tenantId"]), "sectionId": sec["_id"]}
    )
    await db["spec_sections"].delete_one({"_id": sec["_id"]})
    return {"ok": True}

# =====================================================
#                      ПОЗИЦИИ
# =====================================================

# Поля позиции, которые версия сохраняет как «снимок»
ITEM_FIELDS = (
    "pos",          # номер/№
    "name",         # наименование
    "sku",          # артикул
    "vendor",       # поставщик
    "unit",         # ед.изм.
    "qty",          # количество
    "price_work",   # цена работ
    "price_mat",    # цена материала
)

def _calc_total(d: Dict[str, Any]) -> float:
    return _num(d.get("qty"), 0) * (_num(d.get("price_work"), 0) + _num(d.get("price_mat"), 0))

def _active_payload(item: dict) -> dict:
    """
    Срез активной версии с «плоскими» полями для фронта.
    """
    av = int(item.get("activeVersion", 1))
    snap = next(
        (x for x in (item.get("versions") or []) if int(x.get("v")) == av),
        None,
    ) or {}
    data = snap.get("data", {})
    flat = {k: data.get(k) for k in ITEM_FIELDS}
    flat["total"] = data.get("total")
    flat["version"] = int(item.get("version", av))
    flat["activeVersion"] = av
    flat["versions"] = [
        {"v": int(x.get("v")), "savedAt": x.get("savedAt")}
        for x in (item.get("versions") or [])
    ]
    return flat

@router.get("/api/projects/{project_id}/spec/items")
async def spec_items_list(
    project_id: str,
    deleted: Optional[int] = Query(None),
    user=Depends(auth.get_current_user),
):
    q = {"tenantId": _oid(user["tenantId"]), "projectId": _oid(project_id)}
    if deleted is not None:
        q["deleted"] = bool(int(deleted))

    items = (
        await db["spec_items"]
        .find(q)
        .sort([("sectionOrder", 1), ("createdAt", 1)])
        .to_list(100000)
    )

    out: List[dict] = []
    for it in items:
        flat = _active_payload(it)
        out.append({
            **_norm(it),
            **flat,  # pos/name/.../total + activeVersion + (короткий список) versions
            "versions": flat["versions"],
        })
    return {"items": out}

@router.post("/api/projects/{project_id}/spec/items")
async def spec_items_create(
    project_id: str,
    payload: dict = Body(...),
    user=Depends(auth.get_current_user),
):
    """
    Создание позиции: сразу создаётся v1 с ПОЛНЫМИ данными (что пришло).
    Требуется sectionId.
    """
    now = datetime.utcnow()
    section_id = payload.get("sectionId")
    if not section_id:
        raise HTTPException(400, "sectionId is required")

    sec = await db["spec_sections"].find_one(
        {"_id": _oid(section_id), "tenantId": _oid(user["tenantId"])}
    )
    if not sec:
        raise HTTPException(404, "Section not found")

    data = {k: payload.get(k) for k in ITEM_FIELDS}
    data["qty"] = _num(data.get("qty"), 1)
    data["price_work"] = _num(data.get("price_work"), 0)
    data["price_mat"] = _num(data.get("price_mat"), 0)
    data["total"] = _calc_total(data)

    doc = {
        "tenantId": _oid(user["tenantId"]),
        "projectId": _oid(project_id),
        "sectionId": _oid(section_id),
        "sectionOrder": int(sec.get("order") or 0),

        "deleted": False,
        "versions": [{
            "v": 1,
            "data": data,
            "savedAt": now,
            "savedBy": str(user.get("_id")),
        }],
        "activeVersion": 1,
        "version": 1,

        "createdAt": now,
        "updatedAt": now,
    }
    res = await db["spec_items"].insert_one(doc)
    created = await db["spec_items"].find_one({"_id": res.inserted_id})
    flat = _active_payload(created)
    return {**_norm(created), **flat, "versions": flat["versions"]}

@router.patch("/api/spec/items/{item_id}")
async def spec_items_update(
    item_id: str,
    payload: dict = Body(...),
    user=Depends(auth.get_current_user),
):
    """
    Режимы PATCH:
    - commit правок (новая версия):
        {"commit": true, "data": { ... ITEM_FIELDS ... }}
    - переключить активную версию:
        {"setActiveVersion": <int>}
    - мягкое удаление / восстановление:
        {"deleted": true|false}
    - перенос в секцию:
        {"sectionId": "<id>"}   (без версий)
    """
    now = datetime.utcnow()
    it = await db["spec_items"].find_one(
        {"_id": _oid(item_id), "tenantId": _oid(user["tenantId"])}
    )
    if not it:
        raise HTTPException(404, "Item not found")

    updates: Dict[str, Any] = {}

    # soft delete / restore
    if "deleted" in payload:
        updates["deleted"] = bool(payload["deleted"])

    # move to another section
    if "sectionId" in payload and payload.get("sectionId"):
        sec = await db["spec_sections"].find_one(
            {"_id": _oid(payload["sectionId"]), "tenantId": _oid(user["tenantId"])}
        )
        if not sec:
            raise HTTPException(404, "Section not found")
        updates["sectionId"] = sec["_id"]
        updates["sectionOrder"] = int(sec.get("order") or 0)

    # switch active version (без создания новой)
    if "setActiveVersion" in payload:
        v = int(payload["setActiveVersion"])
        if not any(int(x.get("v")) == v for x in (it.get("versions") or [])):
            raise HTTPException(404, f"Version v{v} not found")
        updates["activeVersion"] = v

    # commit → добавить новую версию
    if payload.get("commit"):
        incoming = payload.get("data") or {}
        d = {k: incoming.get(k) for k in ITEM_FIELDS}
        d["qty"] = _num(d.get("qty"), 1)
        d["price_work"] = _num(d.get("price_work"), 0)
        d["price_mat"] = _num(d.get("price_mat"), 0)
        d["total"] = _calc_total(d)

        new_v = int(it.get("version", 1)) + 1
        updates["version"] = new_v
        updates["activeVersion"] = new_v
        updates["versions"] = (it.get("versions") or []) + [{
            "v": new_v,
            "data": d,
            "savedAt": now,
            "savedBy": str(user.get("_id")),
        }]

    if not updates:
        return {**_norm(it), **_active_payload(it)}

    updates["updatedAt"] = now
    await db["spec_items"].update_one({"_id": it["_id"]}, {"$set": updates})
    after = await db["spec_items"].find_one({"_id": it["_id"]})
    flat = _active_payload(after)
    return {**_norm(after), **flat, "versions": flat["versions"]}

@router.delete("/api/spec/items/{item_id}")
async def spec_items_delete_forever(
    item_id: str,
    user=Depends(auth.get_current_user),
):
    it = await db["spec_items"].find_one(
        {"_id": _oid(item_id), "tenantId": _oid(user["tenantId"])}
    )
    if not it:
        raise HTTPException(404, "Item not found")
    await db["spec_items"].delete_one({"_id": it["_id"]})
    return {"ok": True}
