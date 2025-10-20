from fastapi import APIRouter, Depends, HTTPException
from bson import ObjectId
from . import auth
from .db import db

router = APIRouter(prefix="/api/admin/logs", tags=["admin-logs"])

def oid(v):
    try:
        return ObjectId(v)
    except Exception:
        return v

def to_jsonable(x):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]
    return x

@router.get("")
async def list_logs(page: int = 1, limit: int = 100, user=Depends(auth.get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "admin only")

    tid = user["tenantId"]
    q = {"$or": [{"tenantId": oid(tid)}, {"tenantId": str(tid)}]}

    cursor = (db.audit_log
              .find(q)
              .sort([("createdAt", -1)])
              .skip((page - 1) * limit)
              .limit(limit))
    items = []
    async for d in cursor:
        items.append(to_jsonable(d))
    total = await db.audit_log.count_documents(q)
    return {"items": items, "page": page, "limit": limit, "total": total}

@router.delete("")
async def clear_all(user=Depends(auth.get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "admin only")
    tid = user["tenantId"]
    await db.audit_log.delete_many({"$or": [{"tenantId": oid(tid)}, {"tenantId": str(tid)}]})
    return {"ok": True}

@router.delete("/{log_id}")
async def delete_one(log_id: str, user=Depends(auth.get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "admin only")
    await db.audit_log.delete_one({"_id": oid(log_id)})
    return {"ok": True}
