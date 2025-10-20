# app/field_defs.py
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from bson import ObjectId
from datetime import datetime

from .db import collections, db
from .audit import log_action

ENT_RU = {"project": "Проекты", "person": "Сотрудники", "task": "Задачи"}

def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v

async def list_field_defs(entity: str, tenantId: str) -> List[Dict[str, Any]]:
    cur = (
        collections["field"]
        .find({"tenantId": ObjectId(tenantId), "entity": entity})
        .sort([("order", 1), ("_id", 1)])
    )
    out: List[Dict[str, Any]] = []
    async for d in cur:
        d["_id"] = str(d["_id"])
        d["tenantId"] = str(d["tenantId"])
        out.append(d)
    return out


async def delete_field_def(
    entity: str,
    key: str,
    tenant_id: str,
    user: Optional[Dict[str, Any]] = None,   # ⬅ принимаем user
):
    q = {"tenantId": oid(tenant_id), "entity": entity, "key": key}
    before = await collections["field"].find_one(q)
    res = await collections["field"].delete_one(q)

    # логирование
    try:
        author = (user or {}).get("name") or (user or {}).get("email") or "Пользователь"
        label = (before or {}).get("label") or key
        await log_action(
            db,
            tenant_id=str(tenant_id),
            user_id=str((user or {}).get("_id")),      # ⬅ кто
            user_name=(user or {}).get("name"),
            action="field.delete",
            entity="field",
            entity_id=str((before or {}).get("_id")) if before else None,
            message=f'{author} удалил(а) поле «{label}» в разделе «{ENT_RU.get(entity, entity)}»',
        )
    except Exception:
        pass

    return {"ok": bool(getattr(res, "deleted_count", 0))}


async def upsert_field_def(
    payload: Dict[str, Any],
    tenantId: str,
    user: Optional[Dict[str, Any]] = None,   # ⬅ принимаем user
) -> Dict[str, Any]:
    if not payload.get("entity") or not payload.get("key") or not payload.get("type"):
        raise HTTPException(400, "entity, key, type required")

    entity = payload["entity"]
    key = payload["key"]

    q = {"tenantId": ObjectId(tenantId), "entity": entity, "key": key}
    existed = await collections["field"].find_one(q)

    now = datetime.utcnow()
    data = {
        "tenantId": ObjectId(tenantId),
        "entity": entity,
        "key": key,
        "label": payload.get("label") or key,
        "type": payload["type"],
        "required": bool(payload.get("required", False)),
        "indexed": bool(payload.get("indexed", False)),
        "order": int(payload.get("order", 0)),
        "help": payload.get("help", ""),
        "updatedAt": now,
    }
    if not existed:
        data["createdAt"] = now

    await collections["field"].update_one(q, {"$set": data}, upsert=True)
    saved = await collections["field"].find_one(q)

    if data["indexed"]:
        # индекс по extra.<key> в соответствующей коллекции
        await collections[entity].create_index(f"extra.{key}")

    # логирование
    try:
        author = (user or {}).get("name") or (user or {}).get("email") or "Пользователь"
        section = ENT_RU.get(entity, entity)
        action = "field.update" if existed else "field.create"
        msg = (
            f'{author} изменил(а) поле «{data["label"]}» в разделе «{section}»'
            if existed else
            f'{author} создал(а) поле «{data["label"]}» в разделе «{section}»'
        )
        await log_action(
            db,
            tenant_id=str(tenantId),
            user_id=str((user or {}).get("_id")),      # ⬅ кто
            user_name=(user or {}).get("name"),
            action=action,
            entity="field",
            entity_id=str(saved["_id"]),
            message=msg,
            meta={"entity": entity, "key": key},
        )
    except Exception:
        pass

    saved["_id"] = str(saved["_id"])
    saved["tenantId"] = str(saved["tenantId"])
    return saved
