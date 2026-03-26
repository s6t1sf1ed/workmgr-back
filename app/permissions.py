from __future__ import annotations

from typing import Iterable, Any
from fastapi import HTTPException

ALL_PERMISSIONS = [
    "projects.view",
    "projects.create",
    "projects.edit",
    "projects.delete",
    "tasks.view",
    "tasks.create",
    "tasks.edit",
    "tasks.delete",
    "persons.view",
    "persons.create",
    "persons.edit",
    "persons.delete",
    "timesheet.view",
    "fields.view",
    "fields.edit",
    "reports.view",
    "reports.create",
    "reports.edit",
    "reports.delete",
    "project_files.view",
    "project_files.upload",
    "project_files.delete",
    "person_files.view",
    "person_files.upload",
    "person_files.delete",
    "logs.view",
    "logs.manage",
    "users.manage",
]

USER_DEFAULT_PERMISSIONS = [
    "projects.view",
    "projects.create",
    "projects.edit",
    "projects.delete",
    "tasks.view",
    "tasks.create",
    "tasks.edit",
    "tasks.delete",
    "persons.view",
    "persons.create",
    "persons.edit",
    "persons.delete",
    "timesheet.view",
    "fields.view",
    "fields.edit",
    "reports.view",
    "reports.create",
    "reports.edit",
    "reports.delete",
    "project_files.view",
    "project_files.upload",
    "project_files.delete",
    "person_files.view",
    "person_files.upload",
    "person_files.delete",
]

ENTITY_PERMISSION_PREFIX = {
    "projects": "projects",
    "project": "projects",
    "tasks": "tasks",
    "task": "tasks",
    "person": "persons",
    "persons": "persons",
}


def normalize_permissions(perms: Any, role: str | None = None) -> list[str]:
    if role == "admin":
        return ALL_PERMISSIONS[:]

    raw = perms if isinstance(perms, Iterable) and not isinstance(perms, (str, bytes, dict)) else None
    if raw is None:
        return USER_DEFAULT_PERMISSIONS[:]

    out: list[str] = []
    seen = set()
    for x in raw:
        if not isinstance(x, str):
            continue
        p = x.strip()
        if p and p in ALL_PERMISSIONS and p not in seen:
            seen.add(p)
            out.append(p)
    if not out:
        return USER_DEFAULT_PERMISSIONS[:]
    return out


def user_permissions(user: dict) -> list[str]:
    return normalize_permissions(user.get("permissions"), user.get("role"))


def has_permission(user: dict, permission: str) -> bool:
    if user.get("role") == "admin":
        return True
    return permission in user_permissions(user)


def require_permission(user: dict, permission: str, detail: str = "Недостаточно прав") -> None:
    if not has_permission(user, permission):
        raise HTTPException(status_code=403, detail=detail)


def permission_for_entity_action(entity: str, action: str) -> str | None:
    prefix = ENTITY_PERMISSION_PREFIX.get(entity)
    if not prefix:
        return None
    if action == "list" or action == "get":
        return f"{prefix}.view"
    if action == "create":
        return f"{prefix}.create"
    if action == "update":
        return f"{prefix}.edit"
    if action == "delete":
        return f"{prefix}.delete"
    return None


def permissions_catalog() -> list[dict[str, str]]:
    labels = {
        "projects.view": "Проекты: просмотр",
        "projects.create": "Проекты: создание",
        "projects.edit": "Проекты: редактирование",
        "projects.delete": "Проекты: удаление",
        "tasks.view": "Задачи: просмотр",
        "tasks.create": "Задачи: создание",
        "tasks.edit": "Задачи: редактирование",
        "tasks.delete": "Задачи: удаление",
        "persons.view": "Сотрудники: просмотр",
        "persons.create": "Сотрудники: создание",
        "persons.edit": "Сотрудники: редактирование",
        "persons.delete": "Сотрудники: удаление",
        "timesheet.view": "Табель: просмотр",
        "fields.view": "Поля: просмотр",
        "fields.edit": "Поля: изменение",
        "reports.view": "Отчёты: просмотр",
        "reports.create": "Отчёты: создание",
        "reports.edit": "Отчёты: редактирование",
        "reports.delete": "Отчёты: удаление",
        "project_files.view": "Файлы проектов: просмотр",
        "project_files.upload": "Файлы проектов: загрузка",
        "project_files.delete": "Файлы проектов: удаление",
        "person_files.view": "Файлы сотрудников: просмотр",
        "person_files.upload": "Файлы сотрудников: загрузка",
        "person_files.delete": "Файлы сотрудников: удаление",
        "logs.view": "Логи: просмотр",
        "logs.manage": "Логи: управление",
        "users.manage": "Пользователи и доступы: управление",
    }
    return [{"key": key, "label": labels.get(key, key)} for key in ALL_PERMISSIONS]
