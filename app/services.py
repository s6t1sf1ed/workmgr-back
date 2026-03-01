from datetime import datetime
from bson import ObjectId
from typing import Iterable

DEFAULT_PROJECT_NAME = "Без проекта (Inbox)"

def _oid(x):
    if isinstance(x, ObjectId):
        return x
    try:
        return ObjectId(x)
    except Exception:
        return x

async def ensure_default_project(db, tenant_id: str) -> str:
    """ Возвращает id системного проекта для арендатора. Создаёт его, если ещё нет """
    t = _oid(tenant_id)
    p = await db.projects.find_one({"tenantId": t, "isSystem": True})
    if p:
        return str(p["_id"])

    doc = {
        "tenantId": t,
        "name": DEFAULT_PROJECT_NAME,
        "description": "Системный проект для неприкреплённых задач",
        "isSystem": True,
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    res = await db.projects.insert_one(doc)
    return str(res.inserted_id)

ensure_default_project_if_needed = ensure_default_project

# Синхронизация доступов person <-> project

async def sync_person_projects(db, tenant_id: str, person_id: str, project_ids: Iterable[str]) -> dict:

    t = _oid(tenant_id)
    pid = _oid(person_id)
    proj_oids = [ _oid(x) for x in (project_ids or []) ]

    await db.person.update_one(
        {"_id": pid, "tenantId": t},
        {"$set": {"accessProjects": proj_oids, "updatedAt": datetime.utcnow()}}
    )

    all_projects = await db.projects.find({"tenantId": t, "$or": [{"_id": {"$in": proj_oids}}, {"accessPersons": pid}]}).to_list(100000)

    for prj in all_projects:
        has = pid in (prj.get("accessPersons") or [])
        should = prj["_id"] in proj_oids
        if should and not has:
            await db.projects.update_one({"_id": prj["_id"]}, {"$addToSet": {"accessPersons": pid}, "$set": {"updatedAt": datetime.utcnow()}})
        if (not should) and has:
            await db.projects.update_one({"_id": prj["_id"]}, {"$pull": {"accessPersons": pid}, "$set": {"updatedAt": datetime.utcnow()}})

    after_person = await db.person.find_one({"_id": pid})
    return {"ok": True, "person": {"_id": str(pid), "accessProjects": [str(x) for x in after_person.get("accessProjects", [])]}}


async def sync_project_persons(db, tenant_id: str, project_id: str, person_ids: Iterable[str]) -> dict:

    t = _oid(tenant_id)
    prid = _oid(project_id)
    person_oids = [ _oid(x) for x in (person_ids or []) ]

    await db.projects.update_one(
        {"_id": prid, "tenantId": t},
        {"$set": {"accessPersons": person_oids, "updatedAt": datetime.utcnow()}}
    )

    persons = await db.person.find({"tenantId": t, "$or": [{"_id": {"$in": person_oids}}, {"accessProjects": prid}]}).to_list(100000)

    for p in persons:
        has = prid in (p.get("accessProjects") or [])
        should = p["_id"] in person_oids
        if should and not has:
            await db.person.update_one({"_id": p["_id"]}, {"$addToSet": {"accessProjects": prid}, "$set": {"updatedAt": datetime.utcnow()}})
        if (not should) and has:
            await db.person.update_one({"_id": p["_id"]}, {"$pull": {"accessProjects": prid}, "$set": {"updatedAt": datetime.utcnow()}})

    after_project = await db.projects.find_one({"_id": prid})
    return {"ok": True, "project": {"_id": str(prid), "accessPersons": [str(x) for x in after_project.get("accessPersons", [])]}}
