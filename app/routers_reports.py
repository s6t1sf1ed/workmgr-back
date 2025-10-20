from fastapi import APIRouter, Depends, HTTPException, Query, Response
from bson import ObjectId
from io import BytesIO
from openpyxl import Workbook
from datetime import datetime, timezone, timedelta
import re

from .db import db
from . import auth
from .utils import to_jsonable

# ──────────────────────────────────────────────────────────────────────────────
# Время / парсинг

MSK = timezone(timedelta(hours=3))
UTC = timezone.utc

try:
    from dateutil.parser import isoparse as _parse_iso
except Exception:  # pragma: no cover
    _parse_iso = None




def _try_strptime(s: str) -> datetime | None:
    """Пробуем несколько локальных форматов."""
    s = s.strip()
    for fmt in ("%d.%m.%Y, %H:%M", "%d.%m.%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=MSK)
        except Exception:
            pass
    return None


def _as_dt_loose(x) -> datetime | None:
    """
    Любые входные типы -> tz-aware datetime.
    ВАЖНО:
    - datetime без tzinfo (naive) трактуем как UTC (так motor отдает из Mongo)
    - строки без TZ (например '14.10.2025 16:00') считаем МСК
    - ISO с Z/+hh:mm используем как есть
    - epoch -> UTC
    """
    if x is None or x == "":
        return None

    if isinstance(x, datetime):
        # КРИТИЧЕСКОЕ место: naive -> UTC, а НЕ MSK
        return x.replace(tzinfo=UTC) if x.tzinfo is None else x

    if isinstance(x, (int, float)):
        if x > 10_000_000_000:
            x = x / 1000.0
        return datetime.fromtimestamp(float(x), tz=UTC)

    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None

        # локальные форматы: считаем МСК
        dt_local = _try_strptime(s)
        if dt_local:
            return dt_local  # уже МСК

        # ISO (dateutil)
        if _parse_iso:
            try:
                dt = _parse_iso(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=MSK)  # строка без TZ -> МСК
                return dt
            except Exception:
                pass

        # python fromisoformat
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=MSK)
            return dt
        except Exception:
            return None

    return None

def _to_iso_utc(x) -> str | None:
    """
    Для отдачи наружу СТРОГО в UTC (с Z).
    - datetime из БД без tz -> считаем UTC
    - строки без TZ -> считаем МСК, затем в UTC
    """
    if x is None or x == "":
        return None

    if isinstance(x, datetime):
        dt = x if x.tzinfo else x.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    dt = _as_dt_loose(x)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

def _as_utc_for_storage(x) -> datetime | None:
    """К хранению в БД приводим строго к UTC (aware)."""
    dt = _as_dt_loose(x)
    if not dt:
        return None
    return dt.astimezone(UTC)

def _to_msk(x) -> datetime | None:
    """Для отображения приводим к MSK."""
    dt = _as_dt_loose(x)
    if not dt:
        return None
    return dt.astimezone(MSK)


def _fmt_date_time_range(start, end) -> str:
    """'ДД.ММ.ГГГГ\\nHH:MM-HH:MM'."""
    s = _to_msk(start)
    if not s:
        return ""
    e = _to_msk(end) if end else None
    return f"{s.strftime('%d.%m.%Y')}\n{s.strftime('%H:%M')}-{e.strftime('%H:%M') if e else '—'}"


def _fmt_duration(start, end) -> str:
    s = _to_msk(start)
    e = _to_msk(end)
    if not s or not e:
        return ""
    delta = (e - s).total_seconds()
    if delta < 0:
        return ""
    minutes = int(delta // 60)
    return f"{minutes // 60}:{minutes % 60:02d}"


# ──────────────────────────────────────────────────────────────────────────────
# Утилиты / нормализация

router = APIRouter(prefix="/api/reports", tags=["reports"])


def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v


def _norm_id(x):
    return str(x) if isinstance(x, ObjectId) else x


def _fio_from_doc(p: dict | None) -> str:
    if not p:
        return ""
    ln = (p.get("lastName") or p.get("surname") or p.get("last_name") or "").strip()
    fn = (p.get("firstName") or p.get("name") or p.get("first_name") or "").strip()
    if ln or fn:
        return " ".join(x for x in (ln, fn) if x).strip()
    return (p.get("name") or p.get("email") or p.get("telegramId") or p.get("telegram_id") or "").strip()


def _tg_from_doc(p: dict | None) -> str:
    if not p:
        return ""
    v = p.get("telegramId") or p.get("telegram_id") or ""
    return "" if v is None else str(v)


async def _load_person_by_any_id(person_any) -> dict:
    if not person_any:
        return {}
    pid = oid(person_any)
    if isinstance(pid, ObjectId):
        for coll in ("person", "persons", "users"):
            p = await db[coll].find_one({"_id": pid})
            if p:
                return p
    return {}


async def _load_project_by_any_id(project_any) -> dict:
    if not project_any:
        return {}
    pid = oid(project_any)
    if isinstance(pid, ObjectId):
        for coll in ("projects", "project"):
            p = await db[coll].find_one({"_id": pid})
            if p:
                return p
    return {}


def normalize_report(d: dict) -> dict:
    if not d:
        return {}
    out = dict(d)

    out["start_time"] = _to_iso_utc(d.get("start_time"))
    out["end_time"]   = _to_iso_utc(d.get("end_time"))

    out["_id"] = _norm_id(out.get("_id"))
    for k in ("person_id", "personId", "project_id", "projectId",
              "tenantId", "user_id", "company", "companyId", "company_id"):
        if k in out:
            out[k] = _norm_id(out[k])

    p = d.get("_person") or {}
    pr = d.get("_project") or {}
    out["person"] = {
        "id": _norm_id(d.get("person_id") or d.get("personId") or d.get("user_id")),
        "name": _fio_from_doc(p),
        "telegram_id": _tg_from_doc(p),
    }
    out["project"] = {"id": _norm_id(d.get("project_id") or d.get("projectId")), "name": pr.get("name", "") or ""}

    st = d.get("start_time")
    et = d.get("end_time")
    out["timeRangeHuman"] = _fmt_date_time_range(st, et)
    out["workHoursHuman"] = _fmt_duration(st, et)

    out.pop("_person", None)
    out.pop("_project", None)
    return to_jsonable(out)


def _build_archived_filter(archived: int | None):
    if archived is None:
        return {}
    if int(archived) == 1:
        return {"archived": True}
    return {"$or": [{"archived": False}, {"archived": {"$exists": False}}]}


def _match_person(pid_str: str):
    """Матч по сотруднику: поддержка ObjectId/строки/разных полей."""
    oid_val = oid(pid_str)
    str_val = str(pid_str)
    return {
        "$or": [
            {"person_id": oid_val},
            {"personId": oid_val},
            {"user_id": oid_val},
            {"person_id": str_val},
            {"personId": str_val},
            {"user_id": str_val},
            # смешанные типы: поле ObjectId, фильтр строкой (или наоборот)
            {"$expr": {"$eq": [{"$toString": "$person_id"}, str_val]}},
            {"$expr": {"$eq": [{"$toString": "$personId"}, str_val]}},
            {"$expr": {"$eq": [{"$toString": "$user_id"}, str_val]}},
        ]
    }


def _match_project(prj_str: str):
    oid_val = oid(prj_str)
    str_val = str(prj_str)
    return {
        "$or": [
            {"project_id": oid_val},
            {"projectId": oid_val},
            {"project_id": str_val},
            {"projectId": str_val},
            {"$expr": {"$eq": [{"$toString": "$project_id"}, str_val]}},
            {"$expr": {"$eq": [{"$toString": "$projectId"}, str_val]}},
        ]
    }


def _set_dt_if_present(payload: dict, field: str):
    """Если поле есть — приводим к UTC, если пустая строка — очищаем."""
    if field in payload:
        v = payload.get(field)
        dt = _as_utc_for_storage(v)
        if dt is not None:
            payload[field] = dt
        elif v == "" or v is None:
            payload.pop(field, None)
        else:
            # не смогли распарсить — не трогаем исходное
            pass


# ──────────────────────────────────────────────────────────────────────────────
# Роуты

@router.get("")
async def list_reports(
    user=Depends(auth.get_current_user),
    page: int = 1,
    limit: int = 50,
    sort: str = "-start_time",
    archived: int | None = None,
    q: str | None = None,
    person: str | None = Query(None, alias="personId"),
    project: str | None = Query(None, alias="projectId"),
    telegram: str | None = None,
    startFrom: str | None = None,
    startTo: str | None = None,
    endFrom: str | None = None,
    endTo: str | None = None,
    hasPhoto: int | None = None,
    hoursMin: float | None = None,
    hoursMax: float | None = None,
):
    filt: dict = {"tenantId": oid(user["tenantId"])} if user.get("tenantId") else {}

    arch = _build_archived_filter(archived)
    if arch:
        filt.update(arch)

    and_conds = []

    if person:
        and_conds.append(_match_person(person))
    if project:
        and_conds.append(_match_project(project))

    if telegram:
        and_conds.append({"$or": [
            {"telegram_id": {"$regex": telegram, "$options": "i"}},
            {"telegramId": {"$regex": telegram, "$options": "i"}},
        ]})

    if q:
        and_conds.append({"text_report": {"$regex": q, "$options": "i"}})

    dt_from = _as_dt_loose(startFrom) if startFrom else None
    dt_to = _as_dt_loose(startTo) if startTo else None
    if dt_from or dt_to:
        cond = {}
        if dt_from:
            cond["$gte"] = dt_from.astimezone(UTC)
        if dt_to:
            cond["$lte"] = dt_to.astimezone(UTC)
        and_conds.append({"start_time": cond})

    et_from = _as_dt_loose(endFrom) if endFrom else None
    et_to = _as_dt_loose(endTo) if endTo else None
    if et_from or et_to:
        cond = {}
        if et_from:
            cond["$gte"] = et_from.astimezone(UTC)
        if et_to:
            cond["$lte"] = et_to.astimezone(UTC)
        and_conds.append({"end_time": cond})

    if hasPhoto is not None:
        if int(hasPhoto) == 1:
            and_conds.append({"photo_link": {"$exists": True, "$ne": ""}})
        else:
            and_conds.append({"$or": [{"photo_link": ""}, {"photo_link": {"$exists": False}}]})

    exprs = []
    if hoursMin is not None or hoursMax is not None:
        hours_expr = {"$divide": [{"$subtract": ["$end_time", "$start_time"]}, 3600000]}
        if hoursMin is not None:
            exprs.append({"$gte": [hours_expr, float(hoursMin)]})
        if hoursMax is not None:
            exprs.append({"$lte": [hours_expr, float(hoursMax)]})

    if and_conds:
        filt.setdefault("$and", []).extend(and_conds)
    if exprs:
        filt.setdefault("$expr", {"$and": exprs})

    # сортировка стабильная: поле + _id
    s = [("start_time", -1), ("_id", -1)]
    if sort:
        desc = sort.startswith("-")
        fld = sort[1:] if desc else sort
        s = [(fld, -1 if desc else 1), ("_id", -1)]

    total = await db.reports.count_documents(filt)

    MAX_ALL = 5000
    if page == 1:
        # на первой странице отдаём все (до MAX_ALL), чтобы не отрезать «старый хвост»
        effective_limit = min(total, MAX_ALL)
    else:
        # на последующих — запрошенный лимит, но тоже с крышкой
        effective_limit = min(limit, MAX_ALL)

    cur = (
        db.reports.find(filt)
        .sort(s)
        .skip((page - 1) * limit)
        .limit(effective_limit)
    )

    pcache: dict[str, dict] = {}
    prcache: dict[str, dict] = {}
    items: list[dict] = []

    async for r in cur:
        pid = r.get("person_id") or r.get("personId") or r.get("user_id")
        pid_key = str(pid) if isinstance(pid, ObjectId) else str(pid or "")
        if pid and pid_key not in pcache:
            pcache[pid_key] = await _load_person_by_any_id(pid)
        r["_person"] = pcache.get(pid_key, {})

        prid = r.get("project_id") or r.get("projectId")
        prid_key = str(prid) if isinstance(prid, ObjectId) else str(prid or "")
        if prid and prid_key not in prcache:
            prcache[prid_key] = await _load_project_by_any_id(prid)
        r["_project"] = prcache.get(prid_key, {})

        items.append(normalize_report(r))

    return to_jsonable({"items": items, "page": page, "limit": effective_limit, "total": total})


@router.get("/{rid}")
async def get_one(rid: str, user=Depends(auth.get_current_user)):
    d = await db.reports.find_one({"_id": oid(rid), "tenantId": oid(user["tenantId"])})
    if not d:
        raise HTTPException(404, "not found")
    pid = d.get("person_id") or d.get("personId") or d.get("user_id")
    d["_person"] = await _load_person_by_any_id(pid)
    pr = d.get("project_id") or d.get("projectId")
    d["_project"] = await _load_project_by_any_id(pr)
    return to_jsonable(normalize_report(d))


@router.post("")
async def create(payload: dict, user=Depends(auth.get_current_user)):
    _set_dt_if_present(payload, "start_time")
    _set_dt_if_present(payload, "end_time")

    payload["tenantId"] = oid(user["tenantId"]) if user.get("tenantId") else None
    payload["createdAt"] = datetime.now(tz=UTC)
    payload["updatedAt"] = datetime.now(tz=UTC)
    res = await db.reports.insert_one(payload)
    d = await db.reports.find_one({"_id": res.inserted_id})

    pid = d.get("person_id") or d.get("personId") or d.get("user_id")
    d["_person"] = await _load_person_by_any_id(pid)
    pr = d.get("project_id") or d.get("projectId")
    d["_project"] = await _load_project_by_any_id(pr)
    return to_jsonable(normalize_report(d))


@router.patch("/{rid}")
async def update(rid: str, payload: dict, user=Depends(auth.get_current_user)):
    _set_dt_if_present(payload, "start_time")
    _set_dt_if_present(payload, "end_time")

    payload["updatedAt"] = datetime.now(tz=UTC)
    res = await db.reports.update_one(
        {"_id": oid(rid), "tenantId": oid(user["tenantId"])},
        {"$set": payload}
    )
    if res.matched_count == 0:
        raise HTTPException(404, "not found")

    d = await db.reports.find_one({"_id": oid(rid)})
    pid = d.get("person_id") or d.get("personId") or d.get("user_id")
    d["_person"] = await _load_person_by_any_id(pid)
    pr = d.get("project_id") or d.get("projectId")
    d["_project"] = await _load_project_by_any_id(pr)
    return to_jsonable(normalize_report(d))


@router.delete("/{rid}")
async def delete(rid: str, user=Depends(auth.get_current_user)):
    await db.reports.delete_one({"_id": oid(rid), "tenantId": oid(user["tenantId"])})
    return {"ok": True}


@router.get("/export/xlsx")
async def export_xlsx(
    user=Depends(auth.get_current_user),
    archived: int | None = None,
    q: str | None = None,
    person: str | None = Query(None, alias="personId"),
    project: str | None = Query(None, alias="projectId"),
    telegram: str | None = None,
    startFrom: str | None = None,
    startTo: str | None = None,
    endFrom: str | None = None,
    endTo: str | None = None,
    hasPhoto: int | None = None,
    hoursMin: float | None = None,
    hoursMax: float | None = None,
):
    filt: dict = {"tenantId": oid(user["tenantId"])} if user.get("tenantId") else {}
    arch = _build_archived_filter(archived)
    if arch:
        filt.update(arch)

    and_conds = []
    if person:
        and_conds.append(_match_person(person))
    if project:
        and_conds.append(_match_project(project))
    if telegram:
        and_conds.append({"$or": [
            {"telegram_id": {"$regex": telegram, "$options": "i"}},
            {"telegramId": {"$regex": telegram, "$options": "i"}},
        ]})
    if q:
        and_conds.append({"text_report": {"$regex": q, "$options": "i"}})

    dt_from = _as_dt_loose(startFrom) if startFrom else None
    dt_to = _as_dt_loose(startTo) if startTo else None
    if dt_from or dt_to:
        cond = {}
        if dt_from:
            cond["$gte"] = dt_from.astimezone(UTC)
        if dt_to:
            cond["$lte"] = dt_to.astimezone(UTC)
        and_conds.append({"start_time": cond})

    et_from = _as_dt_loose(endFrom) if endFrom else None
    et_to = _as_dt_loose(endTo) if endTo else None
    if et_from or et_to:
        cond = {}
        if et_from:
            cond["$gte"] = et_from.astimezone(UTC)
        if et_to:
            cond["$lte"] = et_to.astimezone(UTC)
        and_conds.append({"end_time": cond})

    if hasPhoto is not None:
        if int(hasPhoto) == 1:
            and_conds.append({"photo_link": {"$exists": True, "$ne": ""}})
        else:
            and_conds.append({"$or": [{"photo_link": ""}, {"photo_link": {"$exists": False}}]})

    exprs = []
    if hoursMin is not None or hoursMax is not None:
        hours_expr = {"$divide": [{"$subtract": ["$end_time", "$start_time"]}, 3600000]}
        if hoursMin is not None:
            exprs.append({"$gte": [hours_expr, float(hoursMin)]})
        if hoursMax is not None:
            exprs.append({"$lte": [hours_expr, float(hoursMax)]})

    if and_conds:
        filt.setdefault("$and", []).extend(and_conds)
    if exprs:
        filt.setdefault("$expr", {"$and": exprs})

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчёты"
    ws.append(["Telegram ID", "Сотрудник", "Проект", "Дата / время", "Часы", "Текст отчёта", "Фото"])

    pcache: dict[str, dict] = {}
    prcache: dict[str, dict] = {}

    async for r in db.reports.find(filt).sort([("start_time", -1), ("_id", -1)]):
        pid = r.get("person_id") or r.get("personId") or r.get("user_id")
        pid_key = str(pid) if isinstance(pid, ObjectId) else str(pid or "")
        if pid and pid_key not in pcache:
            pcache[pid_key] = await _load_person_by_any_id(pid)
        p = pcache.get(pid_key, {})

        prid = r.get("project_id") or r.get("projectId")
        prid_key = str(prid) if isinstance(prid, ObjectId) else str(prid or "")
        if prid and prid_key not in prcache:
            prcache[prid_key] = await _load_project_by_any_id(prid)
        prj = prcache.get(prid_key, {})

        ws.append(
            [
                _tg_from_doc(p),
                _fio_from_doc(p),
                prj.get("name", ""),
                _fmt_date_time_range(r.get("start_time"), r.get("end_time")),
                _fmt_duration(r.get("start_time"), r.get("end_time")),
                r.get("text_report") or "",
                r.get("photo_link") or "",
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    headers = {
        "Content-Disposition": 'attachment; filename="reports.xlsx"',
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    return Response(content=out.getvalue(), headers=headers, media_type=headers["Content-Type"])
