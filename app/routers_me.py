from fastapi import APIRouter, Depends, HTTPException, Body
from passlib.hash import bcrypt
from bson import ObjectId
from .db import db
from . import auth

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
    # user из токена может содержать строковый _id — приводим к ObjectId
    q = {}
    if user.get("_id"):
        q["_id"] = oid(user["_id"])
    elif user.get("email"):
        q["email"] = user["email"]

    u = await db.users.find_one(q)
    if not u:
        # последняя попытка — если токен содержит email
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
    upd = {}
    if "name" in payload:
        upd["name"] = payload["name"]
    if "telegram_id" in payload:
        upd["telegram_id"] = payload["telegram_id"]
    if not upd:
        return {"ok": True}
    await db.users.update_one({"_id": oid(user["_id"])}, {"$set": upd})
    return {"ok": True}

@router.post("/password")
async def change_password(payload: dict = Body(...), user=Depends(auth.get_current_user)):
    cur = payload.get("current_password")
    new = payload.get("new_password")
    u = await db.users.find_one({"_id": oid(user["_id"])})
    if not u or not bcrypt.verify(cur, u.get("password_hash", "")):
        raise HTTPException(400, "Неверный текущий пароль")
    await db.users.update_one({"_id": u["_id"]}, {"$set": {"password_hash": bcrypt.hash(new)}})
    return {"ok": True}

@router.post("/become-admin")
async def become_admin_if_none(user=Depends(auth.get_current_user)):
    tenant_id = oid(user["tenantId"])
    admins = await db.users.count_documents({"tenantId": tenant_id, "role": "admin"})
    if admins > 0:
        raise HTTPException(403, "В компании админ уже есть")
    await db.users.update_one({"_id": oid(user["_id"])}, {"$set": {"role": "admin"}})
    return {"ok": True, "role": "admin"}
