from fastapi import APIRouter, Depends, HTTPException, Body
from passlib.hash import bcrypt
from bson import ObjectId

from .db import db
from . import auth
from .audit import log_user_action, make_diff

router = APIRouter(prefix="/api/me", tags=["me"])


def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


@router.get("")
async def get_me(user=Depends(auth.get_current_user)):
    # приводим к ObjectId
    q = {}
    if user.get("_id"):
        q["_id"] = oid(user["_id"])
    elif user.get("email"):
        q["email"] = user["email"]

    u = await db.users.find_one(q)
    if not u:
        # если токен содержит email
        if user.get("email"):
            u = await db.users.find_one({"email": user["email"]})
    if not u:
        raise HTTPException(404, "Пользователь не найден")

    tenant = None
    if u.get("tenantId"):
        tenant = await db.tenants.find_one({"_id": oid(u["tenantId"])})

    company_name = u.get("companyName") or (tenant.get("name") if tenant else "")

    return {
        "id": str(u["_id"]),
        "name": u.get("name"),
        "login": u.get("login"),
        "telegram_id": u.get("telegram_id"),
        "role": u.get("role", "user"),
        "company": {
            "id": str(u.get("tenantId")) if u.get("tenantId") else None,
            "name": company_name,
        },
    }


@router.patch("")
async def patch_me(payload: dict = Body(...), user=Depends(auth.get_current_user)):
    u = await db.users.find_one({"_id": oid(user["_id"])})
    if not u:
        raise HTTPException(404, "Пользователь не найден")

    upd: dict = {}
    if "name" in payload:
        upd["name"] = payload["name"]
    if "telegram_id" in payload:
        upd["telegram_id"] = payload["telegram_id"]

    if not upd:
        return {"ok": True}

    # diff по редактируемым полям
    before = {
        "name": u.get("name"),
        "telegram_id": u.get("telegram_id"),
    }
    after = {
        "name": upd.get("name", u.get("name")),
        "telegram_id": upd.get("telegram_id", u.get("telegram_id")),
    }
    diff = make_diff(before, after)

    await db.users.update_one({"_id": u["_id"]}, {"$set": upd})

    # логируем
    try:
        await log_user_action(
            db,
            user,
            action="me.update_profile",
            entity="user",
            entity_id=str(u["_id"]),
            message="Изменён профиль пользователя",
            diff=diff or None,
        )
    except Exception:
        pass

    return {"ok": True}


@router.post("/password")
async def change_password(payload: dict = Body(...), user=Depends(auth.get_current_user)):
    cur = payload.get("current_password")
    new = payload.get("new_password")
    u = await db.users.find_one({"_id": oid(user["_id"])})
    if not u:
        raise HTTPException(404, "Пользователь не найден")

    # поддерживаем старое и новое название поля
    stored_hash = u.get("passwordHash") or u.get("password_hash", "")

    if not stored_hash or not bcrypt.verify(cur, stored_hash):
        raise HTTPException(400, "Неверный текущий пароль")

    new_hash = bcrypt.hash(new)

    await db.users.update_one(
        {"_id": u["_id"]},
        {"$set": {
            "passwordHash": new_hash,        # основное поле
            "password_hash": new_hash,       # чтобы старый код не сломался
        }},
    )

    try:
        await log_user_action(
            db,
            user,
            action="me.change_password",
            entity="user",
            entity_id=str(u["_id"]),
            message="Пользователь сменил пароль",
        )
    except Exception:
        pass

    return {"ok": True}


@router.post("/become-admin")
async def become_admin_if_none(user=Depends(auth.get_current_user)):
    tenant_id = oid(user["tenantId"])
    admins = await db.users.count_documents({"tenantId": tenant_id, "role": "admin"})
    if admins > 0:
        raise HTTPException(403, "В компании админ уже есть")

    await db.users.update_one(
        {"_id": oid(user["_id"])},
        {"$set": {"role": "admin"}},
    )

    try:
        await log_user_action(
            db,
            user,
            action="user.become_admin",
            entity="user",
            entity_id=str(user["_id"]),
            message="Пользователь получил права администратора в компании",
        )
    except Exception:
        pass

    return {"ok": True, "role": "admin"}
