from fastapi import APIRouter, Depends, HTTPException, Query, Body
from datetime import datetime, timezone
from bson import ObjectId

from .db import db
from . import auth
from .utils import to_jsonable
from .permissions import require_permission

router = APIRouter(prefix="/api/projects", tags=["worklog"])

UTC = timezone.utc

def oid(x):
    if isinstance(x, ObjectId):
        return x
    try:
        return ObjectId(x)
    except Exception:
        return x

def norm(d: dict) -> dict:
    out = dict(d)
    if "_id" in out: out["_id"] = str(out["_id"])
    if "tenantId" in out and isinstance(out["tenantId"], ObjectId): out["tenantId"] = str(out["tenantId"])
    if "projectId" in out and isinstance(out["projectId"], ObjectId): out["projectId"] = str(out["projectId"])
    if "authorId"  in out and isinstance(out["authorId"], ObjectId):  out["authorId"]  = str(out["authorId"])
    for k in ("createdAt","updatedAt"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].astimezone(UTC).isoformat().replace("+00:00", "Z")
    return out

@router.get("/{project_id}/worklog")
async def list_worklog(project_id: str, date: str = Query(..., description="YYYY-MM-DD"), user=Depends(auth.get_current_user)):
    require_permission(user, "projects.view")
    cur = db.work_logs.find({
        "tenantId": oid(user["tenantId"]),
        "projectId": oid(project_id),
        "date": date,
    }).sort([("createdAt", -1), ("_id", -1)])
    items = [norm(d) async for d in cur]
    return {"items": items}

@router.post("/{project_id}/worklog")
async def add_worklog(project_id: str, payload: dict = Body(...), user=Depends(auth.get_current_user)):
    require_permission(user, "projects.edit")
    text = (payload.get("text") or "").strip()
    date = (payload.get("date") or "").strip()  # YYYY-MM-DD
    if not text or not date:
        raise HTTPException(400, "text and date are required")

    doc = {
        "tenantId": oid(user["tenantId"]),
        "projectId": oid(project_id),
        "date": date,
        "text": text,
        "authorId": oid(user["_id"]),
        "authorName": user.get("name") or user.get("email") or "",
        "createdAt": datetime.now(tz=UTC),
        "updatedAt": datetime.now(tz=UTC),
    }
    ins = await db.work_logs.insert_one(doc)
    saved = await db.work_logs.find_one({"_id": ins.inserted_id})
    return {"item": norm(saved)}

@router.delete("/{project_id}/worklog/{wid}")
async def delete_worklog(project_id: str, wid: str, user=Depends(auth.get_current_user)):
    require_permission(user, "projects.edit")
    res = await db.work_logs.delete_one({
        "_id": oid(wid),
        "tenantId": oid(user["tenantId"]),
        "projectId": oid(project_id),
    })
    if res.deleted_count == 0:
        raise HTTPException(404, "not found")
    return {"ok": True}

@router.get("/{project_id}/worklog/dates")
async def worklog_marked_dates(
    project_id: str,
    from_: str = Query(..., alias="from"),
    to: str   = Query(...),
    user=Depends(auth.get_current_user),
):
    require_permission(user, "projects.view")
    cur = db.work_logs.find({
        "tenantId": oid(user["tenantId"]),
        "projectId": oid(project_id),
        "date": {"$gte": from_, "$lte": to},
    }, projection={"date": 1}).sort([("date", 1)])
    seen = set()
    dates = []
    async for r in cur:
        d = r.get("date")
        if d and d not in seen:
            seen.add(d)
            dates.append(d)
    return {"dates": dates}