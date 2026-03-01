import os
from fastapi import FastAPI, Depends, Body, Request
from fastapi.middleware.cors import CORSMiddleware

from . import auth, crud, field_defs
from .db import db
from .routers_me import router as me_router
from .routers_admin_logs import router as admin_logs_router
from .routers_project_files import router as project_files_router
from .routers_reports import router as reports_router
from .schemas import PersonAccessUpdate, ProjectAccessUpdate
from .services import sync_person_projects, sync_project_persons
from .routers_worklog import router as worklog_router
from .routers_specs import router as specs_router
from .routers_person_files import router as person_files_router

app = FastAPI(title="Work Manager API")

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
if not origins:
    origins = [
        "http://192.168.1.83:4173",
        "http://192.168.1.83:5173",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def ensure_indexes():
    try:
        await db["person"].create_index(
            [("tenantId", 1), ("userId", 1)],
            unique=True,
            name="uniq_tenant_user"
        )
    except Exception:
        pass

    # индексы для быстрого доступа
    try:
        await db["person"].create_index("accessProjects")
        await db["projects"].create_index("accessPersons")
    except Exception:
        pass
    
    try:
        await db["spec_sections"].create_index([("tenantId",1),("projectId",1),("order",1)])
        await db["spec_items"].create_index([("tenantId",1),("projectId",1),("sectionId",1),("archived",1)])
    except Exception:
        pass
    try:
        await db["files"].create_index([("tenantId", 1), ("personId", 1), ("uploadedAt", -1)])
    except Exception:
        pass

app.include_router(auth.router, tags=["auth"])
app.include_router(me_router)
app.include_router(admin_logs_router)
app.include_router(project_files_router)
app.include_router(person_files_router)
app.include_router(reports_router)
app.include_router(worklog_router)
app.include_router(specs_router)

@app.get("/api/meta")
async def meta():
    return {"ok": True}

@app.get("/api/fields/{entity}")
async def list_fields(entity: str, user=Depends(auth.get_current_user)):
    return await field_defs.list_field_defs(entity, user["tenantId"])

@app.post("/api/fields/{entity}")
async def add_field(entity: str, payload: dict = Body(...), user=Depends(auth.get_current_user)):
    payload["entity"] = entity
    return await field_defs.upsert_field_def(payload, user["tenantId"], user)

@app.delete("/api/fields/{entity}/{key}")
async def delete_field(entity: str, key: str, user=Depends(auth.get_current_user)):
    return await field_defs.delete_field_def(entity, key, user["tenantId"], user)


# универсальные CRUD-роуты
@app.get("/api/{entity}")
async def list_entities(
    entity: str,
    q: str | None = None,
    status: str | None = None,
    archived: str | None = None,
    updatedFrom: str | None = None,
    updatedTo: str | None = None,

    projectId: str | None = None,
    project_id: str | None = None,

    page: int = 1,
    limit: int = 25,
    sort: str = "-updatedAt",
    user=Depends(auth.get_current_user),
):
    params = {
        "q": q,
        "status": status,
        "archived": archived,
        "updatedFrom": updatedFrom,
        "updatedTo": updatedTo,

        "projectId": projectId,
        "project_id": project_id,

        "page": page,
        "limit": limit,
        "sort": sort,
    }
    coll = crud.coll_name(entity)
    return await crud.list_entities(db, coll, user, params)

@app.post("/api/{entity}")
async def create(entity: str, payload: dict = Body(...), user=Depends(auth.get_current_user)):
    coll = crud.coll_name(entity)
    return await crud.create_entity(db, coll, user, payload)

@app.get("/api/{entity}/{id}")
async def get_one(entity: str, id: str, user=Depends(auth.get_current_user)):
    coll = crud.coll_name(entity)
    return await crud.get_entity(db, coll, user, id)

@app.patch("/api/{entity}/{id}")
async def update(entity: str, id: str, payload: dict = Body(...), user=Depends(auth.get_current_user)):
    coll = crud.coll_name(entity)
    return await crud.update_entity(db, coll, user, id, payload)

@app.delete("/api/{entity}/{id}")
async def delete(entity: str, id: str, user=Depends(auth.get_current_user)):
    coll = crud.coll_name(entity)
    return await crud.delete_entity(db, coll, user, id)


# синхронизация доступов person <-> project
@app.post("/api/person/{person_id}/access")
async def set_person_access(person_id: str, body: PersonAccessUpdate, user=Depends(auth.get_current_user)):
    return await sync_person_projects(db, user["tenantId"], person_id, body.projectIds)

@app.post("/api/project/{project_id}/access")
async def set_project_access(project_id: str, body: ProjectAccessUpdate, user=Depends(auth.get_current_user)):
    return await sync_project_persons(db, user["tenantId"], project_id, body.personIds)

