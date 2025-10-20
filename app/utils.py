# app/utils.py
from __future__ import annotations
from typing import Any, Dict, List
from bson import ObjectId
from datetime import datetime, date

__all__ = ["oid", "to_jsonable"]

def oid(v: Any) -> Any:
    """
    Безопасно приводит строку к ObjectId.
    Если уже ObjectId — вернёт как есть.
    Если привести нельзя — вернёт исходное значение.
    """
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)  # type: ignore[arg-type]
    except Exception:
        return v

def _norm(v: Any) -> Any:
    """Рекурсивная нормализация для JSON: ObjectId→str, даты→isoformat и т.п."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="ignore")
    if isinstance(v, dict):
        return {k: _norm(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_norm(x) for x in v]
    return v

def to_jsonable(doc: Any) -> Any:
    """
    Применяй перед возвратом через FastAPI, если в ответе есть ObjectId/даты.
    Принимает dict/list/любую структуру.
    """
    return _norm(doc)
