from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    not_started = "not_started"
    in_progress = "in_progress"
    on_hold = "on_hold"
    follow_up = "follow_up"
    blocked = "blocked"
    done = "done"


class TrafficLight(str, Enum):
    green = "green"
    yellow = "yellow"
    red = "red"


class Task(BaseModel):
    id: str
    task_name: str
    definition: str = ""
    task_link: str = ""
    owner: str = ""
    eta: str = ""
    status: TaskStatus = TaskStatus.not_started
    traffic_light: TrafficLight = TrafficLight.green
    latest_update: str = ""
    blockers: List[str] = Field(default_factory=list)
    next_expected_checkpoint: str = ""
    do_not_ask_until: Optional[date] = None
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_from: str = "sprint_plan"


class DailyLogEntry(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_message: str
    applied_changes: List[str] = Field(default_factory=list)
    follow_up_prompts: List[str] = Field(default_factory=list)


class SprintState(BaseModel):
    sprint_name: str = "Current Sprint"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tasks: List[Task] = Field(default_factory=list)
    daily_logs: List[DailyLogEntry] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str
    mode: str = "auto"


class ChatResponse(BaseModel):
    answer: str
    changed_tasks: List[Task] = Field(default_factory=list)
    follow_ups: List[str] = Field(default_factory=list)
    confidence: float = 0.6


class ImportDocxRequest(BaseModel):
    file_path: str
    auto_initialize: bool = True


class FaqItem(BaseModel):
    id: str
    question: str
    answer: str = ""
    archived: bool = False
    archived_at: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
