from datetime import datetime
from typing import Any, Dict, Optional, Mapping
from bson import ObjectId


def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


def make_diff(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """ какие поля изменились """
    diff: Dict[str, Any] = {}
    keys = set(before.keys()) | set(after.keys())
    for k in keys:
        if before.get(k) != after.get(k):
            diff[k] = {"from": before.get(k), "to": after.get(k)}
    return diff


async def log_action(
    db,
    *,
    tenant_id: str,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
    user_email: Optional[str] = None,
    action: str,
    entity: Optional[str] = None,
    entity_id: Optional[str] = None,
    message: str = "",
    diff: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
):
    """
    Пишет запись аудита в коллекцию db.audit_log (Motor).

    Поля:
      - tenantId   - ObjectId арендатора
      - userId     - ObjectId пользователя (если есть)
      - userName   - отображаемое имя пользователя
      - userEmail  - email пользователя (для удобства поиска)
      - action     - машинное имя действия, напр "report.create"
      - entity     - сущность: "report", "project", "spec.item"...
      - entityId   - строковый id сущности
      - message    - человекочитаемое описание
      - diff       - словарь изменений {"field": {"from": ..., "to": ...}}
      - meta       - произвольные доп. данные
      - createdAt  - UTC datetime
    """
    doc = {
        "tenantId": oid(tenant_id),
        "userId": oid(user_id) if user_id else None,
        "userName": user_name,
        "userEmail": user_email,
        "action": action,
        "entity": entity,
        "entityId": entity_id,
        "message": message,
        "diff": diff or None,
        "meta": meta or None,
        "createdAt": datetime.utcnow(),
    }
    doc = {k: v for k, v in doc.items() if v is not None}
    await db.audit_log.insert_one(doc)


def _extract_user_info(user: Mapping[str, Any]) -> Dict[str, Optional[str]]:
    
    """ Достаём из user основные поля для логов """

    uid = user.get("_id") or user.get("id")
    tid = (
        user.get("tenantId")
        or (user.get("company") or {}).get("id")
        or user.get("tenant_id")
    )

    name = user.get("name") or user.get("login") or ""
    email = user.get("email") or user.get("username") or None

    return {
        "user_id": str(uid) if uid else None,
        "tenant_id": str(tid) if tid else None,
        "user_name": name or None,
        "user_email": email,
    }


async def log_user_action(
    db,
    user: Mapping[str, Any],
    *,
    action: str,
    entity: Optional[str] = None,
    entity_id: Optional[str] = None,
    message: str = "",
    diff: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
):

    info = _extract_user_info(user)
    if not info["tenant_id"]:
        # без tenantId выходим
        return

    await log_action(
        db,
        tenant_id=info["tenant_id"],
        user_id=info["user_id"],
        user_name=info["user_name"],
        user_email=info["user_email"],
        action=action,
        entity=entity,
        entity_id=entity_id,
        message=message,
        diff=diff,
        meta=meta,
    )

async def log_project_action(
    db,
    user: Mapping[str, Any],
    project_id: Any,
    *,
    action: str,
    entity: Optional[str] = None,
    entity_id: Optional[str] = None,
    message: str = "",
    diff: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
):
    """ Логирует действие пользователя + подтягивает название проекта и кладёт его в message и meta. """
    
    info = _extract_user_info(user)
    tenant_id = info["tenant_id"]
    if not tenant_id:
        return

    proj_name = ""
    try:
        proj = await db["projects"].find_one(
            {"_id": oid(project_id), "tenantId": oid(tenant_id)}
        )
        if proj:
            proj_name = (proj.get("name") or "").strip()
    except Exception:
        proj = None

    suffix = f' в проекте «{proj_name}»' if proj_name else ""
    meta_all: Dict[str, Any] = {**(meta or {}), "projectId": str(project_id)}
    if proj_name:
        meta_all["projectName"] = proj_name

    await log_action(
        db,
        tenant_id=tenant_id,
        user_id=info["user_id"],
        user_name=info["user_name"],
        user_email=info["user_email"],
        action=action,
        entity=entity,
        entity_id=entity_id,
        message=message + suffix,
        diff=diff,
        meta=meta_all,
    )