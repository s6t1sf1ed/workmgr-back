from typing import Any, Dict, List, Tuple
from fastapi import HTTPException
from bson import ObjectId
from datetime import datetime
import re

from .audit import log_action, make_diff
from .services import ensure_default_project

# соответствие «человек→коллекция в БД»
COLL_MAP = {
    "person":  "person",
    "persons": "persons",
    "project": "projects",
    "task":    "tasks",
}
def coll_name(name: str) -> str:
    return COLL_MAP.get(name, name)

ACCUSATIVE = {
    "project": "проект",
    "task":    "задачу",
    "person":  "сотрудника",
    "field":   "поле",
}

def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v

# ───────── helpers: нормализация ObjectId → str ─────────
def _norm_value(v: Any) -> Any:
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, list):
        return [_norm_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _norm_value(x) for k, x in v.items()}
    return v

def normalize(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc:
        return doc
    return _norm_value(doc)

def human_title(coll: str, doc: dict) -> str:
    if coll == "person":
        fio = " ".join(x for x in [
            doc.get("lastName"),
            doc.get("firstName"),
            doc.get("middleName"),
        ] if x)
        return fio or doc.get("name") or doc.get("email") or str(doc.get("_id"))
    return doc.get("title") or doc.get("name") or str(doc.get("_id"))

async def _backfill_persons_for_tenant(db, tenant_id):
    t_oid = oid(tenant_id)
    users = await db["user"].find({"tenantId": t_oid}).to_list(length=10000)
    created = 0
    for u in users:
        exists = await db["person"].find_one({"tenantId": t_oid, "userId": u["_id"]})
        if exists:
            continue
        doc = {
            "tenantId": t_oid,
            "userId": u["_id"],
            "firstName": u.get("name") or (u.get("email","").split("@")[0] if u.get("email") else ""),
            "email": u.get("email"),
            "archived": False,
            "extra": {},
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }
        res = await db["person"].insert_one(doc)
        try:
            await log_action(
                db,
                tenant_id=str(tenant_id),
                user_id=str(u["_id"]),
                user_name=u.get("name"),
                action="person.create",
                entity="person",
                entity_id=str(res.inserted_id),
                message=f"Автосоздание карточки сотрудника для {u.get('email','')}",
            )
        except Exception:
            pass
        created += 1
    return created

def parse_pagination(q: Dict[str, Any]) -> Tuple[int, int, List[Tuple[str, int]]]:
    page = int(q.get("page", 1))
    limit = min(max(int(q.get("limit", 25)), 1), 250)
    sort = q.get("sort", "-updatedAt")
    sort_parts: List[Tuple[str, int]] = []
    for part in sort.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("-"):
            sort_parts.append((part[1:], -1))
        else:
            sort_parts.append((part, 1))
    if not sort_parts:
        sort_parts = [("updatedAt", -1)]
    return page, limit, sort_parts

_RU2KEY = {
    "новая": "new",
    "новые": "new",
    "новое": "new",
    "в работе": "in_progress",
    "работа": "in_progress",
    "готово": "done",
    "сделано": "done",
}
def _normalize_status(val: Any) -> tuple[str, bool]:
    if not isinstance(val, str):
        return "new", False
    s = val.strip().lower()
    if re.fullmatch(r"архив[а-я]*|archiv(e|ed)?|archive(d)?", s):
        return "new", True
    if s in _RU2KEY:
        return _RU2KEY[s], False
    return s.replace(" ", "_"), False

def _to_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
    return False

# ───────── LIST ─────────
async def list_entities(db, coll: str, user: Dict[str, Any], q: Dict[str, Any]) -> Dict[str, Any]:
    page, limit, sort_parts = parse_pagination(q)

    and_conds: List[Dict[str, Any]] = [
        {"tenantId": oid(user.get("tenantId"))}
    ]

    if "archived" in q and q["archived"] is not None:
        and_conds.append({"archived": _to_bool(q["archived"])})
    else:
        and_conds.append({"$or": [{"archived": False}, {"archived": {"$exists": False}}]})

    qq = q.get("q")
    if qq:
        and_conds.append({
            "$or": [
                {"name": {"$regex": qq, "$options": "i"}},
                {"description": {"$regex": qq, "$options": "i"}},
                {"title": {"$regex": qq, "$options": "i"}},
                {"firstName": {"$regex": qq, "$options": "i"}},
                {"lastName": {"$regex": qq, "$options": "i"}},
                {"middleName": {"$regex": qq, "$options": "i"}},
            ]
        })

    upd: Dict[str, Any] = {}
    if q.get("updatedFrom"):
        upd["$gte"] = datetime.fromisoformat(q["updatedFrom"])
    if q.get("updatedTo"):
        dt = datetime.fromisoformat(q["updatedTo"])
        upd["$lte"] = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    if upd:
        and_conds.append({"updatedAt": upd})

    # важное: фильтр по проекту ДЛЯ ЗАДАЧ
    if coll in ("task", "tasks"):
        raw_pid = q.get("projectId", q.get("project_id"))
        pid: Any
        if isinstance(raw_pid, str):
            pid = raw_pid.strip()
        else:
            pid = raw_pid

        if raw_pid is not None:  # параметр передан
            # inbox = без проекта
            if pid == "" or (isinstance(pid, str) and pid.lower() in {"inbox", "none", "null"}):
                and_conds.append({
                    "$or": [
                        {"projectId": {"$exists": False}},
                        {"projectId": None},
                        {"projectId": ""},
                        {"project_id": {"$exists": False}},
                        {"project_id": None},
                        {"project_id": ""},
                    ]
                })
            else:
                # поддерживаем оба поля
                and_conds.append({
                    "$or": [
                        {"projectId": oid(pid)},
                        {"project_id": oid(pid)},
                    ]
                })

    filt: Dict[str, Any]
    if len(and_conds) == 1:
        filt = and_conds[0]
    else:
        filt = {"$and": and_conds}

    cursor = (
        db[coll]
        .find(filt)
        .sort(sort_parts)
        .skip((page - 1) * limit)
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)
    total = await db[coll].count_documents(filt)

    if coll in ("person",) and total == 0:
        created_cnt = await _backfill_persons_for_tenant(db, user.get("tenantId"))
        if created_cnt:
            cursor = (
                db[coll]
                .find(filt)
                .sort(sort_parts)
                .skip((page - 1) * limit)
                .limit(limit)
            )
            docs = await cursor.to_list(length=limit)
            total = await db[coll].count_documents(filt)

    items = [normalize(d) for d in docs]
    return {"items": items, "page": page, "limit": limit, "total": total}

# ───────── CREATE ─────────
async def create_entity(db, coll: str, user: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.utcnow()

    if coll in ("task", "tasks"):
        # приводим к единому имени поля
        if "project_id" in data and not data.get("projectId"):
            data["projectId"] = data.pop("project_id")

        status_val = data.get("status") or data.get("statusKey") or "new"
        norm, set_arch = _normalize_status(status_val)
        data["status"] = norm
        if set_arch:
            data["archived"] = True
        data.pop("statusKey", None)

        if not data.get("projectId"):
            default_pid = await ensure_default_project(db, user["tenantId"])
            data["projectId"] = oid(default_pid)
        else:
            data["projectId"] = oid(data["projectId"])

    doc = {
        **data,
        "tenantId": oid(user.get("tenantId")),
        "archived": bool(data.get("archived", False)),
        "createdAt": now,
        "updatedAt": now,
    }
    if "extra" not in doc or not isinstance(doc["extra"], dict):
        doc["extra"] = {}

    res = await db[coll].insert_one(doc)
    created = await db[coll].find_one({"_id": res.inserted_id})
    out = normalize(created)

    try:
        singular = coll[:-1] if coll.endswith("s") else coll
        title = human_title(singular, created)
        noun  = ACCUSATIVE.get(singular, singular)
        await log_action(
            db,
            tenant_id=user["tenantId"],
            user_id=str(user.get("_id")),
            user_name=user.get("name"),
            action=f"{coll}.create",
            entity=coll,
            entity_id=out["_id"],
            message=f'Пользователь {user.get("name")} создал(а) {noun} «{title}»',
            meta={"coll": coll},
        )
    except Exception:
        pass

    return out

# ───────── READ ─────────
async def get_entity(db, coll: str, user: Dict[str, Any], _id: str) -> Dict[str, Any]:
    doc = await db[coll].find_one({"_id": oid(_id), "tenantId": oid(user.get("tenantId"))})
    if not doc:
        raise HTTPException(status_code=404, detail="Not found")
    return normalize(doc)

# ───────── UPDATE ─────────
async def update_entity(db, coll: str, user: Dict[str, Any], _id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    data = {k: v for k, v in data.items() if k not in {"_id", "tenantId", "createdAt"}}
    if coll == "person":
        data.pop("userId", None)

    if coll in ("task", "tasks"):
        # унифицируем projectId
        if "project_id" in data and not data.get("projectId"):
            data["projectId"] = data.pop("project_id")

        if ("status" in data) or ("statusKey" in data):
            status_val = data.get("status") or data.get("statusKey")
            if status_val is not None:
                norm, set_arch = _normalize_status(status_val)
                data["status"] = norm
                if set_arch:
                    data["archived"] = True
            data.pop("statusKey", None)

        if "projectId" in data:
            if data.get("projectId"):
                data["projectId"] = oid(data["projectId"])
            else:
                default_pid = await ensure_default_project(db, user["tenantId"])
                data["projectId"] = oid(default_pid)

    data["updatedAt"] = datetime.utcnow()

    before = await db[coll].find_one({"_id": oid(_id), "tenantId": oid(user.get("tenantId"))})
    if not before:
        raise HTTPException(status_code=404, detail="Not found")

    await db[coll].update_one({"_id": before["_id"]}, {"$set": data})
    after = await db[coll].find_one({"_id": before["_id"]})
    out = normalize(after)

    try:
        diff = make_diff(before, after)
        if diff:
            title = human_title(coll[:-1] if coll.endswith("s") else coll, after)
            noun  = ACCUSATIVE.get(coll[:-1] if coll.endswith("s") else coll, coll)

            if "archived" in diff and bool(before.get("archived")) != bool(after.get("archived")):
                if after.get("archived"):
                    action  = f"{coll}.archive"
                    message = f'Пользователь {user.get("name")} перенес(ла) {noun} «{title}» в архив'
                else:
                    action  = f"{coll}.unarchive"
                    message = f'Пользователь {user.get("name")} вернул(а) {noun} «{title}» из архива'
            else:
                action  = f"{coll}.update"
                message = f'Пользователь {user.get("name")} изменил(а) {noun} «{title}»'

            await log_action(
                db,
                tenant_id=user["tenantId"],
                user_id=str(user.get("_id")),
                user_name=user.get("name"),
                action=action,
                entity=coll,
                entity_id=out["_id"],
                message=message,
                diff=diff,
            )
    except Exception:
        pass

    return out

# ───────── DELETE ─────────
async def delete_entity(db, coll: str, user: Dict[str, Any], _id: str) -> Dict[str, Any]:
    d = await db[coll].find_one({"_id": oid(_id), "tenantId": oid(user.get("tenantId"))})
    if not d:
        raise HTTPException(status_code=404, detail="Not found")

    await db[coll].delete_one({"_id": oid(_id)})

    try:
        title = human_title(coll[:-1] if coll.endswith("s") else coll, d)
        noun  = ACCUSATIVE.get(coll[:-1] if coll.endswith("s") else coll, coll)
        await log_action(
            db,
            tenant_id=user["tenantId"],
            user_id=str(user.get("_id")),
            user_name=user.get("name"),
            action=f"{coll}.delete",
            entity=coll,
            entity_id=str(_id),
            message=f'Пользователь {user.get("name")} удалил(а) {noun} «{title}»',
        )
    except Exception:
        pass
    return {"ok": True}
