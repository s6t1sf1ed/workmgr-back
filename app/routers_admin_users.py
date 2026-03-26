from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException

from . import auth
from .audit import log_user_action, make_diff
from .db import db
from .permissions import permissions_catalog, normalize_permissions, require_permission, user_permissions

router = APIRouter(prefix="/api/admin/users", tags=["admin-users"])


def oid(v: Any):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


def to_jsonable(x: Any):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [to_jsonable(v) for v in x]
    return x


@router.get("/permissions")
async def list_permissions(user=Depends(auth.get_current_user)):
    require_permission(user, "users.manage")
    return {
        "items": permissions_catalog(),
        "defaults": {
            "admin": user_permissions({"role": "admin"}),
            "user": user_permissions({"role": "user", "permissions": None}),
        },
    }


@router.get("")
async def list_users(user=Depends(auth.get_current_user)):
    require_permission(user, "users.manage")

    tid = oid(user["tenantId"])
    cur = db.users.find({"tenantId": tid}).sort([("role", 1), ("name", 1), ("email", 1)])
    items = []
    async for doc in cur:
        doc = to_jsonable(doc)
        doc["permissions"] = user_permissions(doc)
        items.append({
            "_id": doc.get("_id"),
            "name": doc.get("name"),
            "email": doc.get("email"),
            "role": doc.get("role", "user"),
            "companyName": doc.get("companyName"),
            "permissions": doc.get("permissions", []),
            "createdAt": doc.get("createdAt"),
            "updatedAt": doc.get("updatedAt"),
        })
    return {"items": items}


@router.patch("/{user_id}")
async def update_user_access(user_id: str, payload: dict = Body(...), user=Depends(auth.get_current_user)):
    require_permission(user, "users.manage")

    target = await db.users.find_one({"_id": oid(user_id), "tenantId": oid(user["tenantId"])})
    if not target:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    if target.get("role") == "admin":
        raise HTTPException(status_code=400, detail="Изменение прав администратора запрещено")

    upd: dict[str, Any] = {}
    if "permissions" in payload:
        upd["permissions"] = normalize_permissions(payload.get("permissions"), role="user")
    if "name" in payload:
        upd["name"] = (payload.get("name") or "").strip() or target.get("name")

    if not upd:
        return {"ok": True}

    upd["updatedAt"] = datetime.utcnow()
    before = {**target}
    await db.users.update_one({"_id": target["_id"]}, {"$set": upd})
    after = await db.users.find_one({"_id": target["_id"]})
    out = to_jsonable(after)
    out["permissions"] = user_permissions(out)

    try:
        diff = make_diff(before, after)
        await log_user_action(
            db,
            user,
            action="users.permissions.update",
            entity="user",
            entity_id=str(target["_id"]),
            message=f'Пользователь {user.get("name")} изменил(а) права доступа пользователя {target.get("email")}',
            diff=diff,
        )
    except Exception:
        pass

    return {
        "ok": True,
        "item": {
            "_id": out.get("_id"),
            "name": out.get("name"),
            "email": out.get("email"),
            "role": out.get("role", "user"),
            "permissions": out.get("permissions", []),
            "updatedAt": out.get("updatedAt"),
        },
    }
