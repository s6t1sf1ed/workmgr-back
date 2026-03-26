from datetime import datetime, timedelta
from typing import Optional, Tuple
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from bson import ObjectId
import re, secrets

from .audit import log_action
from .db import db
from .db import collections, settings
from .permissions import user_permissions

router = APIRouter(prefix="/auth", tags=["auth"])
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# утилиты

def _split_fio(name: str) -> Tuple[str, str, str]:
    parts = (name or "").strip().split()
    last  = parts[0] if len(parts) > 0 else ""
    first = parts[1] if len(parts) > 1 else ""
    mid   = " ".join(parts[2:]) if len(parts) > 2 else ""
    return last, first, mid

def oid(v):
    if isinstance(v, ObjectId):
        return v
    try:
        return ObjectId(v)
    except Exception:
        return v

def split_name(full: str) -> dict:
    s = (full or "").strip()
    if not s:
        return {"firstName": "", "lastName": "", "middleName": ""}
    parts = [p for p in s.split() if p]
    if len(parts) == 1:
        return {"firstName": parts[0], "lastName": "", "middleName": ""}
    if len(parts) == 2:
        return {"lastName": parts[0], "firstName": parts[1], "middleName": ""}
    return {"lastName": parts[0], "firstName": parts[1], "middleName": " ".join(parts[2:])}

def norm_email(s: str) -> str:
    return (s or "").strip().lower()

def company_slug(name: str) -> str:
    s = re.sub(r"\s+", " ", (name or "").strip())
    s = s.lower().replace("ё", "e")
    return re.sub(r"[^a-z0-9]+", "-", s)[:64]

def make_join_code() -> str:
    return secrets.token_hex(3)

def make_token(user: dict) -> str:
    payload = {
        "sub": str(user["_id"]),
        "tenantId": str(user["tenantId"]),
        "role": user.get("role", "user"),
        "permissions": user_permissions(user),
        "exp": datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRE_MIN),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm="HS256")

async def _ensure_person_for_user(user_doc: dict) -> None:

    """ Если у пользователя в рамках его компании нет карточки в коллекции person создаём её """

    tid = user_doc["tenantId"]
    uid = user_doc["_id"]
    person = await collections["person"].find_one({"tenantId": tid, "userId": uid})
    if person:
        return

    last, first, mid = _split_fio(user_doc.get("name") or "")
    now = datetime.utcnow()
    person_doc = {
        "tenantId": tid,
        "userId": uid,
        "lastName": last,
        "firstName": first or user_doc.get("name") or "",
        "middleName": mid,
        "email": user_doc.get("email"),
        "archived": False,
        "extra": {},
        "createdAt": now,
        "updatedAt": now,
    }
    ins = await collections["person"].insert_one(person_doc)

    # лог
    try:
        await log_action(
            db,
            tenant_id=str(tid),
            user_id=str(uid),
            user_name=user_doc.get("name"),
            action="person.create",
            entity="person",
            entity_id=str(ins.inserted_id),
            message=f"Создана карточка сотрудника для {user_doc.get('email')}",
        )
    except Exception:
        pass

async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    try:
        data = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    uid = data.get("sub")
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = await collections["user"].find_one({"_id": oid(uid)})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    user["_id"] = str(user["_id"])
    user["tenantId"] = str(user["tenantId"])
    user["permissions"] = user_permissions(user)
    return user

# схема 

class RegisterCompanyIn(BaseModel):
    email: EmailStr
    password: str
    company: str
    name: Optional[str] = None

class RegisterEmployeeIn(BaseModel):
    email: EmailStr
    password: str
    name: str
    company: str

class LoginIn(BaseModel):
    email: EmailStr
    password: str

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/register-company", response_model=TokenOut)
async def register_company(body: RegisterCompanyIn):
    # компания уникальна по имени
    exists = await collections["tenant"].find_one({"name": body.company})
    if exists:
        raise HTTPException(400, "Компания с таким названием уже существует")

    tenant_doc = {
        "name": body.company,
        "slug": company_slug(body.company),
        "joinCode": make_join_code(),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    t_res = await collections["tenant"].insert_one(tenant_doc)

    email = norm_email(body.email)
    if await collections["user"].find_one({"email": email}):
        raise HTTPException(400, "Пользователь с таким email уже существует")

    user_doc = {
        "email": email,
        "name": (body.name or email.split("@")[0]).strip(),
        "passwordHash": pwd.hash(body.password),
        "tenantId": t_res.inserted_id,
        "companyName": body.company,
        "role": "admin",
        "permissions": user_permissions({"role": "admin"}),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    u_res = await collections["user"].insert_one(user_doc)

    # карточка сотрудника (владелец тоже в "Сотрудниках")
    fio = split_name(user_doc["name"])
    person_doc = {
        "tenantId": t_res.inserted_id,
        "userId": u_res.inserted_id,
        **fio,
        "email": email,
        "telegramId": "",
        "position": "",
        "archived": False,
        "extra": {},
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    await collections["person"].insert_one(person_doc)

    # аудит
    try:
        tid = str(t_res.inserted_id)
        uid = str(u_res.inserted_id)
        uname = user_doc["name"]
        await log_action(
            db, tenant_id=tid, user_id=uid, user_name=uname,
            action="company.create", entity="tenant", entity_id=tid,
            message=f"Создана компания «{tenant_doc['name']}»",
        )
        await log_action(
            db, tenant_id=tid, user_id=uid, user_name=uname,
            action="user.create", entity="user", entity_id=uid,
            message=f"Создан владелец (admin) {email}",
        )
        await log_action(
            db, tenant_id=tid, user_id=uid, user_name=uname,
            action="person.create", entity="person", entity_id=uid,
            message=f"Создана карточка сотрудника для {email}",
        )
    except Exception:
        pass

    token = make_token({**user_doc, "_id": u_res.inserted_id})
    return TokenOut(access_token=token)

@router.post("/register", response_model=TokenOut)
async def register_employee(body: RegisterEmployeeIn):
    # ищем компанию по joinCode или по имени
    t = await collections["tenant"].find_one({
        "$or": [
            {"joinCode": body.company},
            {"name": {"$regex": f"^{re.escape(body.company)}$", "$options": "i"}},
        ]
    })
    if not t:
        raise HTTPException(404, "Компания не найдена (проверьте название или код приглашения)")

    email = norm_email(body.email)
    if await collections["user"].find_one({"email": email}):
        raise HTTPException(400, "Пользователь с таким email уже существует")

    user_doc = {
        "email": email,
        "name": body.name.strip(),
        "passwordHash": pwd.hash(body.password),
        "tenantId": t["_id"],
        "companyName": t["name"],
        "role": "user",
        "permissions": user_permissions({"role": "user", "permissions": None}),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    u_res = await collections["user"].insert_one(user_doc)

    # карточка сотрудника
    fio = split_name(user_doc["name"])
    person_doc = {
        "tenantId": t["_id"],
        "userId": u_res.inserted_id,
        **fio,
        "email": email,
        "telegramId": "",
        "position": "",
        "archived": False,
        "extra": {},
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    }
    p_res = await collections["person"].insert_one(person_doc)

    # аудит
    try:
        tid = str(t["_id"])
        uid = str(u_res.inserted_id)
        uname = user_doc["name"]
        await log_action(
            db, tenant_id=tid, user_id=uid, user_name=uname,
            action="user.create", entity="user", entity_id=uid,
            message=f"Зарегистрирован сотрудник {email}",
        )
        await log_action(
            db, tenant_id=tid, user_id=uid, user_name=uname,
            action="person.create", entity="person", entity_id=str(p_res.inserted_id),
            message=f"Создана карточка сотрудника {email}",
        )
    except Exception:
        pass

    token = make_token({**user_doc, "_id": u_res.inserted_id})
    return TokenOut(access_token=token)

# алиас, если фронт шлёт на /auth/register-employee
@router.post("/register-employee", response_model=TokenOut)
async def register_employee_alias(body: RegisterEmployeeIn):
    return await register_employee(body)

@router.post("/login", response_model=TokenOut)
async def login(body: LoginIn):
    email = norm_email(body.email)
    user = await collections["user"].find_one({"email": email})
    if not user or not pwd.verify(body.password, user.get("passwordHash", "")):
        raise HTTPException(400, "Invalid credentials")

    await _ensure_person_for_user(user)

    # аудит входа
    try:
        await log_action(
            db, tenant_id=str(user["tenantId"]),
            user_id=str(user["_id"]), user_name=user.get("name"),
            action="auth.login", entity="user", entity_id=str(user["_id"]),
            message=f"Вход пользователя {email}",
        )
    except Exception:
        pass

    return TokenOut(access_token=make_token(user))

@router.post("/signin", response_model=TokenOut)
async def signin_alias(body: LoginIn):
    return await login(body)

@router.get("/me")
async def me(user=Depends(get_current_user)):
    tenant = await collections["tenant"].find_one({"_id": oid(user["tenantId"])}, projection={"name": 1})
    return {"email": user["email"], "tenantId": user["tenantId"], "company": (tenant or {}).get("name", "")}
