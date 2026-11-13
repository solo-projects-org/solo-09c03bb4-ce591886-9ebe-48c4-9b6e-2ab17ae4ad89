"""Outbound/inbound payload models for the HTTP API."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class ConsentUpdate(BaseModel):
    consented: bool = Field(..., description="true = subject consents, false = revokes")


class SubjectView(BaseModel):
    subject_id: str
    consented: bool
    consented_since: Optional[str]
    last_changed_at: str
    revocation_epoch: int


class ConsentResult(SubjectView):
    changed: bool


class BatchEvent(BaseModel):
    at: str
    kind: str
    detail: Optional[str]


class BatchView(BaseModel):
    batch_id: str
    created_at: str
    status: Literal["running", "completed", "failed", "invalidated"]
    failure_reason: Optional[str]
    finished_at: Optional[str]
    row_count: Optional[int]
    downloadable: bool


class BatchDetail(BatchView):
    events: list[BatchEvent]


class BatchList(BaseModel):
    batches: list[BatchView]


class SubjectList(BaseModel):
    subjects: list[SubjectView]


class HealthView(BaseModel):
    status: Literal["ok"]
    boot_id: str


class ErrorBody(BaseModel):
    error: str
    message: str
