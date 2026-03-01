from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "workmgr"
    JWT_SECRET: str = "CHANGE_ME"
    JWT_EXPIRE_MIN: int = 60 * 24 * 7

settings = Settings()

client = AsyncIOMotorClient(settings.MONGO_URI)
db = client[settings.MONGO_DB]

collections = {
    "project": db.projects,
    "person": db.person,
    "task": db.tasks,
    "field": db.entity_field_defs,
    "user": db.users,
    "tenant": db.tenants,
    "counter": db.counters,
    "report": db.reports,
    "report_photo": db.report_photos,
    "files": db.files,
    "audit_log": db.audit_log,
    "work_logs": db.work_logs,
}

async def ensure_indexes():
    await collections["project"].create_index([("tenantId", 1)])
    await collections["person"].create_index(
        [("tenantId", 1), ("userId", 1)],
        unique=True,
        name="uniq_tenant_user",
    )
    await collections["task"].create_index([("tenantId", 1), ("status", 1)])
    await collections["task"].create_index([("tenantId", 1), ("archived", 1)])
    await collections["field"].create_index([("tenantId", 1), ("entity", 1), ("key", 1)], unique=True)
    await collections["counter"].create_index([("tenantId", 1), ("entity", 1)], unique=True)
    await collections["project"].create_index("name")
    await collections["user"].create_index("email", unique=True)
    await collections["report"].create_index([("tenantId", 1), ("archived", 1), ("start_time", -1)])
    await collections["report"].create_index([("tenantId", 1), ("user_id", 1), ("start_time", -1)])
    await collections["report"].create_index([("tenantId", 1), ("project_id", 1), ("start_time", -1)])

    # Логи действий (для вкладки Логи у админа)
    await db.audit_logs.create_index([("tenantId", 1), ("createdAt", -1)])

    # Файлы проектов
    await db.files.create_index([("tenantId", 1), ("projectId", 1), ("uploadedAt", -1)])

    # Задачи: быстрый фильтр по проекту и свежести
    await collections["task"].create_index([("tenantId", 1), ("projectId", 1), ("createdAt", -1)])
    await collections["task"].create_index([("tenantId", 1), ("updatedAt", -1)])


    # системный "Без проекта (Inbox)" + поиск по имени в пределах арендатора
    await collections["project"].create_index([("tenantId", 1), ("isSystem", 1)])
    await collections["project"].create_index([("tenantId", 1), ("name", 1)])

    # фильтр по роли внутри компании
    await collections["user"].create_index([("tenantId", 1), ("role", 1)])

    await collections["project"].create_index([("tenantId", 1), ("ask_location", 1)])
    await collections["project"].create_index([("tenantId", 1), ("latitude", 1), ("longitude", 1)])

    await collections["work_logs"].create_index([("tenantId",1),("projectId",1),("date",1)])
    await collections["work_logs"].create_index([("tenantId",1),("projectId",1),("createdAt",-1)])

    await collections["spec_items"].create_index([("tenantId", 1), ("sectionId", 1), ("parentId", 1), ("pos", 1)],name="spec_items_parent_pos")
    await collections["spec_items"].create_index([("tenantId", 1), ("sectionId", 1), ("path", 1)],name="spec_items_path")