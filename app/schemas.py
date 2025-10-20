from typing import Any, Literal, Optional, Dict, List
from pydantic import BaseModel, Field
from datetime import datetime

Entity = Literal['project','person','task','assignment']

class ExtraModel(BaseModel):
    extra: Dict[str, Any] = Field(default_factory=dict)

class ProjectIn(ExtraModel):
    name: str
    description: Optional[str] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    # список сотрудников с доступом (many-to-many)
    accessPersons: Optional[List[str]] = None

class PersonIn(ExtraModel):
    firstName: str
    lastName: Optional[str] = None
    middleName: Optional[str] = None
    projectId: Optional[str] = None
    # список проектов с доступом (many-to-many)
    accessProjects: Optional[List[str]] = None

class TaskIn(ExtraModel):
    projectId: str
    title: str
    description: Optional[str] = None
    date: Optional[datetime] = None
    status: Literal['new','in_progress','done'] = 'new'

class AssignmentIn(ExtraModel):
    personId: str
    taskId: str
    role: Optional[str] = None

class FieldDefIn(BaseModel):
    entity: Entity
    key: str
    label: str
    type: Literal['string','text','number','bool','date','select','multiselect'] = 'string'
    required: bool = False
    options: Optional[list[dict]] = None
    default: Optional[Any] = None
    indexed: bool = False
    unique: bool = False
    order: int = 0
    help: Optional[str] = None

class ReportIn(BaseModel):
    user_id: str | None = None
    project_id: str | None = None
    telegram_id: str | None = None
    start_time: datetime
    end_time: datetime | None = None
    text_report: str | None = None
    photo_link: str | None = None
    entry_location_method: str | None = None
    exit_location_method: str | None = None
    archived: bool = False

# ——— модели для синхронизации доступов ———

class PersonAccessUpdate(BaseModel):
    projectIds: List[str] = Field(default_factory=list)

class ProjectAccessUpdate(BaseModel):
    personIds: List[str] = Field(default_factory=list)
