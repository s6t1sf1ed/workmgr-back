from datetime import datetime
from typing import Any, Dict, Optional
from bson import ObjectId

def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v

def make_diff(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """Очень простой diff по верхнему уровню."""
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
    action: str,
    entity: Optional[str] = None,
    entity_id: Optional[str] = None,
    message: str = "",
    diff: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
):
    """
    Пишет запись аудита в коллекцию db.audit_log (Motor).
    """
    doc = {
        "tenantId": oid(tenant_id),
        "userId": oid(user_id) if user_id else None,
        "userName": user_name,
        "action": action,
        "entity": entity,
        "entityId": entity_id,
        "message": message,
        "diff": diff or None,
        "meta": meta or None,
        "createdAt": datetime.utcnow(),
    }
    # убираем None-поля
    doc = {k: v for k, v in doc.items() if v is not None}
    await db.audit_log.insert_one(doc)
