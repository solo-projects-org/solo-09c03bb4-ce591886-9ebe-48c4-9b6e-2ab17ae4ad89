"""HTTP surface: consent registry, batch generation, batch download."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .config import Settings
from .schemas import (
    BatchDetail,
    BatchList,
    BatchView,
    ConsentResult,
    ConsentUpdate,
    ErrorBody,
    HealthView,
    SubjectList,
    SubjectView,
)
from .service import ApiError, Service

_TAGS = [
    {"name": "consent", "description": "Read and change subject consent. Changes apply immediately."},
    {"name": "batches", "description": "Start de-identified generation batches and inspect their state."},
    {"name": "download", "description": "Download a completed, still-valid batch CSV."},
    {"name": "ops", "description": "Liveness probe."},
]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    service = Service(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        service.shutdown()

    app = FastAPI(
        lifespan=lifespan,
        title="LIMS De-identification Batch API",
        version="1.0.0",
        description=(
            "Single-instance de-identification service for a fixed LIMS extract.\n\n"
            "A batch replaces subject ids with study codes that are stable across batches; "
            "all other values and row order are unchanged. Revoking consent permanently "
            "invalidates every batch that included that subject, including batches already "
            "generated, and later re-consent does not restore them."
        ),
        openapi_tags=_TAGS,
    )
    app.state.service = service

    @app.exception_handler(ApiError)
    async def _api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "message": exc.message},
        )

    @app.get("/healthz", response_model=HealthView, tags=["ops"], summary="Liveness probe")
    def healthz() -> HealthView:
        return HealthView(status="ok", boot_id=service.boot_id)

    @app.get(
        "/subjects",
        response_model=SubjectList,
        tags=["consent"],
        summary="List subject consent state",
    )
    def list_subjects() -> SubjectList:
        return SubjectList(subjects=[SubjectView(**s) for s in service.list_subjects()])

    @app.get(
        "/subjects/{subject_id}/consent",
        response_model=SubjectView,
        tags=["consent"],
        summary="Read one subject's consent state",
        responses={404: {"model": ErrorBody, "description": "Unknown subject"}},
    )
    def get_consent(subject_id: str) -> SubjectView:
        return SubjectView(**service.get_subject(subject_id))

    @app.put(
        "/subjects/{subject_id}/consent",
        response_model=ConsentResult,
        tags=["consent"],
        summary="Grant or revoke consent",
        description=(
            "Revocation immediately and permanently invalidates every batch that admitted "
            "the subject, whether still generating or completed, and deletes its file. "
            "Re-granting later does not revive those batches."
        ),
    )
    def set_consent(subject_id: str, body: ConsentUpdate) -> ConsentResult:
        return ConsentResult(**service.set_consent(subject_id, body.consented))

    @app.post(
        "/batches",
        response_model=BatchDetail,
        status_code=202,
        tags=["batches"],
        summary="Start a de-identification batch",
        description=(
            "Admits every subject consented at this moment, then generates asynchronously. "
            "Consent changes made during generation are saved but do not alter admission; "
            "a revocation invalidates the batch instead."
        ),
    )
    def create_batch() -> BatchDetail:
        return BatchDetail(**service.create_batch())

    @app.get("/batches", response_model=BatchList, tags=["batches"], summary="List batches")
    def list_batches() -> BatchList:
        return BatchList(batches=[BatchView(**b) for b in service.list_batches()])

    @app.get(
        "/batches/{batch_id}",
        response_model=BatchDetail,
        tags=["batches"],
        summary="Inspect a batch",
        responses={404: {"model": ErrorBody, "description": "Unknown batch"}},
    )
    def get_batch(batch_id: str) -> BatchDetail:
        return BatchDetail(**service.get_batch(batch_id))

    @app.get(
        "/batches/{batch_id}/download",
        tags=["download"],
        summary="Download the generated CSV",
        description=(
            "Only a completed batch with an intact, verified file is served. "
            "Invalidated batches return 410 permanently; failed or unfinished batches 409. "
            "A download that has already started is allowed to finish."
        ),
        response_class=StreamingResponse,
        responses={
            200: {"content": {"text/csv": {}}, "description": "Generated CSV"},
            404: {"model": ErrorBody, "description": "Unknown batch"},
            409: {"model": ErrorBody, "description": "Batch not finished or failed"},
            410: {"model": ErrorBody, "description": "Batch permanently invalidated"},
        },
    )
    def download(batch_id: str) -> StreamingResponse:
        filename, size, handle = service.open_download(batch_id)

        def stream():
            try:
                while True:
                    chunk = handle.read(65536)
                    if not chunk:
                        break
                    yield chunk
            finally:
                handle.close()

        return StreamingResponse(
            stream(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(size),
                "Cache-Control": "no-store",
            },
        )

    return app
