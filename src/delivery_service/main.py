from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Annotated, AsyncIterator, Literal
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from delivery_service.config import get_settings
from delivery_service.commands import (
    CreateOrderCommand,
    TransitionOrderCommand,
    UpdateOrderCommand,
)
from delivery_service.cache import RedisCache, build_redis_client
from delivery_service.background import PreviewCache
from delivery_service.concurrency_lab import router as concurrency_lab_router
from delivery_service.db import engine, transactional_session
from delivery_service.error_contract import (
    CONCURRENT_UPDATE,
    COURIER_ALREADY_BUSY,
    HTTP_ERROR_SPECS,
    IDEMPOTENCY_CONFLICT,
    INTERNAL_ERROR,
    INVALID_PROOF,
    INVALID_TRANSITION,
    ORDER_NOT_FOUND,
    REQUEST_INVALID,
    UPLOAD_TOO_LARGE,
    ErrorSpec,
    error_response,
)
from delivery_service.observability import (
    TelemetryMiddleware,
    configure_otlp_exporter,
    configure_json_logging,
    flush_tracing,
    log_event,
)
from delivery_service.identity import (
    IdentityRepository, InMemoryIdentityRepository, SqlAlchemyIdentityRepository,
)
from delivery_service.integrations import verify_webhook_signature
from delivery_service.outbox import WebhookEventConflict, accept_payment_webhook_once
from delivery_service.policy import (
    audit_trail,
    login_limit_keys,
    login_rate_limiter,
    normalize_login_subject,
    subject_fingerprint,
    trusted_client_network_key,
)
from delivery_service.presentation import OrderCardView
from delivery_service.repository import (
    CourierAlreadyBusy, ConcurrentUpdate, IdempotencyConflict,
    InMemoryOrderRepository,
    OrderRepository,
    SqlAlchemyOrderRepository,
)
from delivery_service.refresh_sessions import (
    InMemoryRefreshSessionRepository,
    RefreshSession,
    RefreshSessionRepository,
    RotationResult,
    SqlAlchemyRefreshSessionRepository,
)
from delivery_service.domain import InvalidTransition, OrderStatus
from delivery_service.schemas import (
    ErrorResponse, OrderCreate, OrderRead, OrderTransition, OrderUpdate, Registration,
    TokenPair, TrackingRead, TrackingUpdate,
)
from delivery_service.security import (
    BEARER_CHALLENGE, DUMMY_PASSWORD_HASH, actor_from_claims, check_password,
    create_token, current_actor, decode_token, hash_password, issue_token_pair, owns_order,
    parse_subject,
)
from delivery_service.csrf import CSRF_COOKIE, SESSION_COOKIE, new_csrf_token
from delivery_service.access import Actor, Role
from delivery_service.service import OrderNotFound, OrderService
from delivery_service.storage import (
    InMemoryProofStorage,
    InMemoryProofMetadataRepository,
    InvalidProofLink,
    InvalidUpload,
    ProofLinkSigner,
    ProofMetadataRepository,
    SqlAlchemyProofMetadataRepository,
    UploadTooLarge,
    inspect_proof,
)
from delivery_service.tracking import (
    InMemoryTrackingSnapshotRepository,
    SqlAlchemyTrackingSnapshotRepository,
    TrackingEvent,
    TrackingHub,
    TrackingSnapshotRepository,
    order_event_stream,
)
from delivery_service.http_client import build_route_http_client
from delivery_service.webhooks import (
    InMemoryWebhookInbox,
    PaymentWebhookEnvelope,
    timestamp_is_fresh,
)


repository = InMemoryOrderRepository()
identity_repository = InMemoryIdentityRepository()
refresh_session_repository = InMemoryRefreshSessionRepository()
payment_webhook_inbox = InMemoryWebhookInbox()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
proof_storage = InMemoryProofStorage()
proof_metadata_repository = InMemoryProofMetadataRepository()
proof_link_signer = ProofLinkSigner(
    get_settings().proof_link_secret.get_secret_value(),
    max_ttl_seconds=get_settings().proof_link_ttl_seconds,
)
preview_cache = PreviewCache()
tracking_snapshots = InMemoryTrackingSnapshotRepository()
TRACKING_CHANNEL = "delivery.tracking.position"


def _login_context(request: Request, raw_subject: str) -> tuple[str, str, tuple[str, str]]:
    """Return bounded login input and two independent abuse-control keys."""
    try:
        subject = normalize_login_subject(raw_subject)
    except ValueError:
        # Keep invalid identifiers cheap and indistinguishable at the HTTP
        # boundary.  The network budget still prevents rotating junk inputs.
        subject = "invalid-subject"
    network_key = trusted_client_network_key(
        request.client.host if request.client else None
    )
    return subject, network_key, login_limit_keys(
        client_network_key=network_key, subject=subject,
    )


async def _record_auth_event(
    request: Request,
    *,
    actor_id: str | None,
    subject: str,
    action: str,
    outcome: str,
    reason_code: str,
    network_key: str | None = None,
) -> None:
    await audit_trail.record(
        actor_id=actor_id,
        subject_fingerprint=subject_fingerprint(subject),
        action=action,
        resource_id=None,
        outcome=outcome,
        reason_code=reason_code,
        client_network_key=network_key or trusted_client_network_key(
            request.client.host if request.client else None
        ),
        correlation_id=getattr(request.state, "correlation_id", ""),
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_json_logging()
    configure_otlp_exporter(settings.otel_exporter_otlp_endpoint)
    app.state.http_client = build_route_http_client(settings)
    app.state.redis_client = build_redis_client(
        settings.redis_url,
        connect_timeout_seconds=settings.redis_connect_timeout_seconds,
        socket_timeout_seconds=settings.redis_socket_timeout_seconds,
        max_connections=settings.redis_max_connections,
    )
    app.state.cache = RedisCache(app.state.redis_client)
    app.state.tracking = TrackingHub(queue_size=settings.tracking_queue_size)
    app.state.tracking_listener = None
    if not settings.use_in_memory_repository:
        app.state.tracking_listener = asyncio.create_task(_consume_tracking_bus(app))
    app.state.ready = False
    try:
        if not settings.use_in_memory_repository:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        app.state.ready = True
        yield
    finally:
        app.state.ready = False
        if app.state.tracking_listener is not None:
            app.state.tracking_listener.cancel()
            await asyncio.gather(app.state.tracking_listener, return_exceptions=True)
        await app.state.http_client.aclose()
        await app.state.redis_client.aclose()
        if not settings.use_in_memory_repository:
            await engine.dispose()
        flush_tracing()


app = FastAPI(
    title="Delivery Service",
    version="1.0.0",
    lifespan=lifespan,
    responses={
        422: {"model": ErrorResponse, "description": "Request validation failed"},
        500: {"model": ErrorResponse, "description": "Unexpected server error"},
    },
)
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.allowed_origins),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=[
        "Authorization", "Content-Type", "Idempotency-Key", "X-CSRF-Token",
        "X-Request-ID",
    ],
    expose_headers=["Location", "X-Request-ID"],
    max_age=600,
)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=list(settings.allowed_hosts),
    www_redirect=False,
)
app.add_middleware(TelemetryMiddleware)
api_router = APIRouter()

if settings.environment != "production":
    app.include_router(concurrency_lab_router)


async def get_order_repository() -> AsyncIterator[OrderRepository]:
    if get_settings().use_in_memory_repository:
        yield repository
        return
    async with transactional_session() as session:
        yield SqlAlchemyOrderRepository(session)


def get_order_service(
    order_repository: Annotated[OrderRepository, Depends(get_order_repository)],
) -> OrderService:
    return OrderService(order_repository)


async def get_identity_repository() -> AsyncIterator[IdentityRepository]:
    if get_settings().use_in_memory_repository:
        yield identity_repository
        return
    async with transactional_session() as session:
        yield SqlAlchemyIdentityRepository(session)


async def get_refresh_session_repository() -> AsyncIterator[RefreshSessionRepository]:
    if get_settings().use_in_memory_repository:
        yield refresh_session_repository
        return
    async with transactional_session() as session:
        yield SqlAlchemyRefreshSessionRepository(session)


async def get_proof_metadata_repository() -> AsyncIterator[ProofMetadataRepository]:
    if get_settings().use_in_memory_repository:
        yield proof_metadata_repository
        return
    async with transactional_session() as session:
        yield SqlAlchemyProofMetadataRepository(session)


async def get_tracking_snapshot_repository() -> AsyncIterator[TrackingSnapshotRepository]:
    if get_settings().use_in_memory_repository:
        yield tracking_snapshots
        return
    async with transactional_session() as session:
        yield SqlAlchemyTrackingSnapshotRepository(session)


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    request: Request, error: RequestValidationError,
) -> JSONResponse:
    details = [
        {
            "location": list(item["loc"]),
            "type": item["type"],
            "message": item["msg"],
        }
        for item in error.errors()
    ]
    return error_response(request, REQUEST_INVALID, details=details)


@app.exception_handler(OrderNotFound)
async def order_not_found_handler(request: Request, _error: OrderNotFound) -> JSONResponse:
    """Exception handler преобразует известное исключение в HTTP-ответ."""
    return error_response(request, ORDER_NOT_FOUND)


@app.exception_handler(InvalidUpload)
async def invalid_upload_handler(request: Request, _error: InvalidUpload) -> JSONResponse:
    return error_response(request, INVALID_PROOF)


@app.exception_handler(UploadTooLarge)
async def upload_too_large_handler(request: Request, _error: UploadTooLarge) -> JSONResponse:
    return error_response(request, UPLOAD_TOO_LARGE)


@app.exception_handler(ConcurrentUpdate)
async def concurrent_update_handler(request: Request, _error: ConcurrentUpdate) -> JSONResponse:
    return error_response(request, CONCURRENT_UPDATE)


@app.exception_handler(CourierAlreadyBusy)
async def courier_already_busy_handler(
    request: Request, _error: CourierAlreadyBusy,
) -> JSONResponse:
    return error_response(request, COURIER_ALREADY_BUSY)


@app.exception_handler(IdempotencyConflict)
async def idempotency_conflict_handler(
    request: Request, _error: IdempotencyConflict,
) -> JSONResponse:
    return error_response(request, IDEMPOTENCY_CONFLICT)


@app.exception_handler(InvalidTransition)
async def invalid_transition_handler(request: Request, _error: InvalidTransition) -> JSONResponse:
    return error_response(request, INVALID_TRANSITION)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request, error: StarletteHTTPException,
) -> JSONResponse:
    spec = HTTP_ERROR_SPECS.get(
        error.status_code,
        ErrorSpec(error.status_code, "http_error", "Request could not be completed"),
    )
    return error_response(request, spec, headers=error.headers)


@app.exception_handler(Exception)
async def unexpected_error_handler(request: Request, error: Exception) -> JSONResponse:
    request_id = getattr(request.state, "correlation_id", "")
    route = getattr(request.scope.get("route"), "path", "/unmatched")
    log_event(
        "unhandled_request_error",
        level=logging.ERROR,
        exc_info=error,
        correlation_id=request_id,
        method=request.method,
        route=route,
        error_kind=type(error).__name__,
    )
    return error_response(request, INTERNAL_ERROR)


@api_router.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@api_router.get("/health/ready", tags=["health"])
async def readiness(request: Request) -> dict[str, str]:
    if not getattr(request.app.state, "ready", False):
        raise HTTPException(status_code=503, detail="not ready")
    return {"status": "ready"}


@api_router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@api_router.post("/api/v1/auth/token", response_model=TokenPair)
async def issue_token(
    request: Request,
    response: Response,
    form: Annotated[OAuth2PasswordRequestForm, Depends()],
    identities: Annotated[IdentityRepository, Depends(get_identity_repository)],
    refresh_sessions: Annotated[
        RefreshSessionRepository, Depends(get_refresh_session_repository)
    ],
) -> TokenPair:
    subject, network_key, limit_keys = _login_context(request, form.username)
    limit = await login_rate_limiter.check_many(limit_keys)
    if not limit.allowed:
        await _record_auth_event(
            request, actor_id=None, subject=subject, action="auth.login",
            outcome="rate_limited", reason_code="attempt_budget_exhausted",
            network_key=network_key,
        )
        raise HTTPException(
            status_code=429,
            detail="too many login attempts",
            headers={"Retry-After": str(limit.retry_after_seconds)},
        )
    password_hash = await identities.password_hash(subject)
    bounded_password = form.password if len(form.password) <= 1024 else "invalid-password"
    password_check = check_password(
        bounded_password, password_hash or DUMMY_PASSWORD_HASH,
    )
    if password_hash is None or not password_check.verified or len(form.password) > 1024:
        await _record_auth_event(
            request, actor_id=None, subject=subject, action="auth.login",
            outcome="denied", reason_code="invalid_credentials",
            network_key=network_key,
        )
        raise HTTPException(status_code=401, detail="invalid credentials")
    if password_check.replacement_hash is not None:
        await identities.replace_password_hash(
            subject,
            expected=password_hash,
            replacement=password_check.replacement_hash,
        )
    parse_subject(subject)
    tenant_id = await identities.tenant_id(subject)
    if tenant_id is None:
        raise HTTPException(status_code=401, detail="invalid credentials")
    pair = issue_token_pair(subject, tenant_id=tenant_id)
    await refresh_sessions.register(
        RefreshSession(
            jti=pair.refresh_jti,
            family_id=pair.family_id,
            subject=subject,
            expires_at=pair.refresh_expires_at,
        )
    )
    await _record_auth_event(
        request, actor_id=subject, subject=subject, action="auth.login",
        outcome="allowed", reason_code="credentials_verified",
        network_key=network_key,
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return TokenPair(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.access_expires_in,
    )


@api_router.post("/api/v1/auth/refresh", response_model=TokenPair)
async def rotate_refresh_token(
    request: Request,
    response: Response,
    grant_type: Annotated[str, Form(pattern="^refresh_token$")],
    refresh_token: Annotated[str, Form(min_length=20, max_length=4096)],
    refresh_sessions: Annotated[
        RefreshSessionRepository, Depends(get_refresh_session_repository)
    ],
) -> TokenPair:
    claims = decode_token(refresh_token, expected_type="refresh")
    subject = str(claims["sub"])
    family_id = UUID(str(claims["family"]))
    presented_jti = UUID(str(claims["jti"]))
    pair = issue_token_pair(
        subject,
        family_id=family_id,
        tenant_id=UUID(str(claims["tenant"])),
    )
    result = await refresh_sessions.rotate(
        presented_jti=presented_jti,
        family_id=family_id,
        subject=subject,
        replacement=RefreshSession(
            jti=pair.refresh_jti,
            family_id=pair.family_id,
            subject=subject,
            expires_at=pair.refresh_expires_at,
        ),
        now=datetime.now(UTC),
    )
    if result is not RotationResult.ROTATED:
        await _record_auth_event(
            request,
            actor_id=subject,
            subject=subject,
            action="auth.refresh",
            outcome="reuse_detected" if result is RotationResult.REUSED else "denied",
            reason_code=(
                "refresh_reuse_detected"
                if result is RotationResult.REUSED
                else "invalid_refresh_session"
            ),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid refresh token",
            headers=BEARER_CHALLENGE,
        )

    await _record_auth_event(
        request, actor_id=subject, subject=subject, action="auth.refresh",
        outcome="rotated", reason_code="refresh_rotated",
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return TokenPair(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.access_expires_in,
    )


@api_router.post("/api/v1/auth/browser-session", status_code=status.HTTP_204_NO_CONTENT)
async def create_browser_session(
    request: Request,
    response: Response,
    form: Annotated[OAuth2PasswordRequestForm, Depends()],
    identities: Annotated[IdentityRepository, Depends(get_identity_repository)],
) -> None:
    """Exchange first-party credentials for a hardened browser cookie session."""
    subject, network_key, raw_keys = _login_context(request, form.username)
    limit_keys = tuple(key.replace("login:", "browser-login:", 1) for key in raw_keys)
    limit = await login_rate_limiter.check_many(limit_keys)
    if not limit.allowed:
        await _record_auth_event(
            request, actor_id=None, subject=subject, action="auth.browser_login",
            outcome="rate_limited", reason_code="attempt_budget_exhausted",
            network_key=network_key,
        )
        raise HTTPException(
            status_code=429, detail="too many login attempts",
            headers={"Retry-After": str(limit.retry_after_seconds)},
        )
    password_hash = await identities.password_hash(subject)
    bounded_password = form.password if len(form.password) <= 1024 else "invalid-password"
    password_check = check_password(bounded_password, password_hash or DUMMY_PASSWORD_HASH)
    if password_hash is None or not password_check.verified or len(form.password) > 1024:
        await _record_auth_event(
            request, actor_id=None, subject=subject, action="auth.browser_login",
            outcome="denied", reason_code="invalid_credentials",
            network_key=network_key,
        )
        raise HTTPException(status_code=401, detail="invalid credentials")
    parse_subject(subject)
    tenant_id = await identities.tenant_id(subject)
    if tenant_id is None:
        raise HTTPException(status_code=401, detail="invalid credentials")

    access_token = create_token(
        subject,
        token_type="access",
        lifetime=timedelta(minutes=15),
        tenant_id=tenant_id,
    )
    csrf_token = new_csrf_token(str(decode_token(access_token)["jti"]))
    response.set_cookie(
        SESSION_COOKIE, access_token, max_age=900, secure=True, httponly=True,
        samesite="lax", path="/",
    )
    response.set_cookie(
        CSRF_COOKIE, csrf_token, max_age=900, secure=True, httponly=False,
        samesite="lax", path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    await _record_auth_event(
        request, actor_id=subject, subject=subject, action="auth.browser_login",
        outcome="allowed", reason_code="credentials_verified",
        network_key=network_key,
    )


@api_router.delete("/api/v1/auth/browser-session", status_code=status.HTTP_204_NO_CONTENT)
async def delete_browser_session(
    response: Response,
    _actor: Annotated[Actor, Depends(current_actor)],
) -> None:
    """Clear both browser cookies; current_actor enforces CSRF first."""
    response.delete_cookie(SESSION_COOKIE, path="/", secure=True, httponly=True, samesite="lax")
    response.delete_cookie(CSRF_COOKIE, path="/", secure=True, httponly=False, samesite="lax")
    response.headers["Cache-Control"] = "no-store"


@api_router.post("/api/v1/auth/register", status_code=status.HTTP_201_CREATED)
async def register_identity(
    registration: Registration,
    identities: Annotated[IdentityRepository, Depends(get_identity_repository)],
) -> dict[str, str]:
    subject = normalize_login_subject(registration.subject)
    parse_subject(subject)
    if not await identities.add(subject, hash_password(registration.password)):
        raise HTTPException(status_code=409, detail="identity already exists")
    return {"subject": subject}


@api_router.post("/api/v1/webhooks/payment", status_code=status.HTTP_202_ACCEPTED)
async def payment_webhook(
    request: Request,
    event_id_header: Annotated[str, Header(alias="X-Event-ID", min_length=36, max_length=36)],
    timestamp_header: Annotated[str, Header(alias="X-Webhook-Timestamp", min_length=1, max_length=20)],
    signature: Annotated[str, Header(alias="X-Webhook-Signature", min_length=64, max_length=64)],
) -> dict[str, str]:
    settings = get_settings()
    content_length = request.headers.get("content-length")
    if content_length and content_length.isdecimal() and int(content_length) > settings.webhook_max_bytes:
        raise HTTPException(status_code=413, detail="webhook payload is too large")
    payload = await request.body()
    if len(payload) > settings.webhook_max_bytes:
        raise HTTPException(status_code=413, detail="webhook payload is too large")
    if not verify_webhook_signature(
        payload, signature, settings.webhook_secret.get_secret_value().encode("utf-8"),
        timestamp=timestamp_header,
    ):
        raise HTTPException(status_code=401, detail="invalid webhook signature")
    if not timestamp_is_fresh(
        timestamp_header,
        tolerance_seconds=settings.webhook_tolerance_seconds,
    ):
        raise HTTPException(status_code=401, detail="invalid webhook signature")
    if request.headers.get("content-type", "").split(";", 1)[0].strip().casefold() != "application/json":
        raise HTTPException(status_code=415, detail="webhook requires application/json")
    try:
        envelope = PaymentWebhookEnvelope.model_validate_json(payload)
        event_id = UUID(event_id_header)
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="invalid webhook payload") from None
    if event_id != envelope.id:
        raise HTTPException(status_code=422, detail="webhook event ID mismatch")

    durable_payload = envelope.durable_payload(payload)
    try:
        if settings.use_in_memory_repository:
            applied = await payment_webhook_inbox.accept(event_id, durable_payload)
        else:
            async with transactional_session() as session:
                applied = await accept_payment_webhook_once(
                    session,
                    event_id=event_id,
                    event_payload=durable_payload,
                )
    except WebhookEventConflict as exc:
        raise HTTPException(status_code=409, detail="webhook event ID conflict") from exc
    await audit_trail.record(
        actor_id="payment-provider",
        subject_fingerprint=subject_fingerprint("payment-provider"),
        action="payment_webhook",
        resource_id=str(event_id),
        outcome="accepted" if applied else "duplicate",
        reason_code="signature_verified",
        client_network_key=trusted_client_network_key(
            request.client.host if request.client else None
        ),
        correlation_id=getattr(request.state, "correlation_id", ""),
    )
    return {"status": "accepted" if applied else "duplicate"}


@api_router.post(
    "/api/v1/orders",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createOrderV1",
    responses={
        201: {
            "description": "Order created",
            "headers": {
                "Location": {
                    "description": "URI of the created order",
                    "schema": {"type": "string"},
                }
            },
        },
        409: {"model": ErrorResponse, "description": "Idempotency conflict"},
    },
)
async def create_order(
    command: OrderCreate,
    service: Annotated[OrderService, Depends(get_order_service)],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=8,
            max_length=128,
            pattern=r"^[A-Za-z0-9._:-]+$",
        ),
    ],
    response: Response,
    actor: Annotated[Actor, Depends(current_actor)],
) -> OrderRead:
    if actor.role is not Role.CUSTOMER or actor.user_id != command.customer_id:
        raise HTTPException(status_code=403, detail="cannot create an order for another customer")
    order = OrderRead.model_validate(await service.create(
        CreateOrderCommand(
            customer_id=command.customer_id,
            pickup_address=command.pickup_address,
            destination_address=command.destination_address,
            weight_grams=command.weight_grams,
        ),
        idempotency_key=idempotency_key,
        tenant_id=actor.tenant_id,
    ))
    response.headers["Location"] = f"/api/v1/orders/{order.id}"
    return order


@api_router.get("/api/v1/orders/{order_id}", response_model=OrderRead)
async def get_order(
    order_id: UUID,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> OrderRead:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    return OrderRead.model_validate(order)


@api_router.post(
    "/api/v1/orders/{order_id}/preview/rebuild",
    status_code=status.HTTP_202_ACCEPTED,
)
async def rebuild_order_preview(
    order_id: UUID,
    tasks: BackgroundTasks,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> dict[str, str]:
    """Schedule a disposable projection only after checking order ownership."""
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    tasks.add_task(preview_cache.rebuild, order.id, order.version)
    return {"status": "accepted"}


@api_router.get(
    "/pages/orders/{order_id}",
    response_class=HTMLResponse,
    name="get_order_card",
)
async def get_order_card(
    request: Request,
    order_id: UUID,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> HTMLResponse:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    response = templates.TemplateResponse(
        request=request,
        name="orders/card.html",
        context={"order": OrderCardView.from_order(order)},
    )
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


@api_router.patch("/api/v1/orders/{order_id}", response_model=OrderRead)
async def update_order(
    order_id: UUID,
    command: OrderUpdate,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
    expected_version: Annotated[int, Header(alias="X-Expected-Version", ge=1)],
) -> OrderRead:
    current = await service.get(order_id)
    if not owns_order(actor, tenant_id=current.tenant_id, customer_id=current.customer_id, courier_id=current.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    return OrderRead.model_validate(await service.update(
        order_id,
        UpdateOrderCommand(**command.model_dump(exclude_unset=True, exclude_none=True)),
        expected_version=expected_version,
    ))


@api_router.post("/api/v1/orders/{order_id}/transitions", response_model=OrderRead)
async def transition_order(
    order_id: UUID,
    command: OrderTransition,
    request: Request,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
    expected_version: Annotated[int, Header(alias="X-Expected-Version", ge=1)],
) -> OrderRead:
    current = await service.get(order_id)
    if not owns_order(actor, tenant_id=current.tenant_id, customer_id=current.customer_id, courier_id=current.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    if actor.role is Role.CUSTOMER and (
        actor.user_id != current.customer_id or command.status != OrderStatus.CANCELLED
    ):
        raise HTTPException(status_code=403, detail="customer may only cancel an own order")
    return OrderRead.model_validate(await service.transition(
        order_id,
        TransitionOrderCommand(
            target=command.status,
            expected_version=expected_version,
            actor_id=actor.user_id,
            actor_role=actor.role.value,
            correlation_id=getattr(request.state, "correlation_id", ""),
        ),
    ))


@api_router.post(
    "/api/v1/courier/orders/claim",
    response_model=OrderRead,
    responses={204: {"description": "No unlocked order is currently available"}},
)
async def claim_next_order(
    request: Request,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> OrderRead | Response:
    if actor.role is not Role.COURIER:
        raise HTTPException(status_code=403, detail="only couriers may claim orders")
    claimed = await service.claim_next(
        courier_id=actor.user_id,
        tenant_id=actor.tenant_id,
        correlation_id=getattr(request.state, "correlation_id", ""),
    )
    if claimed is None:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return OrderRead.model_validate(claimed)


@api_router.get("/api/v1/orders", response_model=list[OrderRead])
async def list_orders(
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
    sort: Annotated[str, Query(pattern="^(created_at|status)$")] = "created_at",
    status_filter: Annotated[
        str | None, Query(alias="status", pattern="^(created|assigned|picked_up|delivered|cancelled)$")
    ] = None,
) -> list[OrderRead]:
    orders = await service.list_visible(
        role=actor.role.value, actor_id=actor.user_id, tenant_id=actor.tenant_id,
        limit=limit, offset=offset,
        sort=sort, status_filter=status_filter,
    )
    return [OrderRead.model_validate(order) for order in orders]


@api_router.post("/api/v1/orders/{order_id}/proofs", status_code=status.HTTP_201_CREATED)
async def upload_proof(
    order_id: UUID,
    response: Response,
    file: Annotated[UploadFile, File(description="PNG or JPEG delivery proof")],
    proof_kind: Annotated[Literal["pickup", "delivery"], Form()],
    service: Annotated[OrderService, Depends(get_order_service)],
    proof_metadata: Annotated[
        ProofMetadataRepository, Depends(get_proof_metadata_repository)
    ],
    actor: Annotated[Actor, Depends(current_actor)],
) -> dict[str, str | int]:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    validated = await inspect_proof(
        file,
        tenant_id=actor.tenant_id,
        order_id=order_id,
        max_bytes=get_settings().max_upload_bytes,
    )
    proof = await proof_storage.put_private(validated)
    await proof_metadata.add(
        proof,
        proof_kind=proof_kind,
        created_at=datetime.now(UTC),
    )
    signed = proof_link_signer.issue_get(
        proof.key,
        now=datetime.now(UTC),
        ttl_seconds=get_settings().proof_link_ttl_seconds,
    )
    response.headers["Location"] = f"/api/v1/orders/{order_id}/proofs/{proof.sha256}"
    return {
        "key": proof.key,
        "proof_kind": proof_kind,
        "media_type": proof.media_type,
        "size": proof.size,
        "sha256": proof.sha256,
        "download_url": signed.url,
        "download_expires_at": signed.expires_at.isoformat(),
    }


@api_router.get("/api/v1/orders/{order_id}/proofs/{sha256}")
async def download_proof(
    order_id: UUID,
    sha256: str,
    service: Annotated[OrderService, Depends(get_order_service)],
    proof_metadata: Annotated[
        ProofMetadataRepository, Depends(get_proof_metadata_repository)
    ],
    actor: Annotated[Actor, Depends(current_actor)],
) -> Response:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    proof = await proof_metadata.find(
        tenant_id=actor.tenant_id,
        order_id=order_id,
        sha256=sha256,
    )
    if proof is None:
        raise HTTPException(status_code=404, detail="proof not found")
    stored = await proof_storage.get_private(proof.key)
    if stored is None:
        raise HTTPException(status_code=404, detail="proof object not found")
    content, media_type = stored
    if media_type != proof.media_type:
        raise HTTPException(status_code=500, detail="proof metadata mismatch")
    suffix = ".png" if proof.media_type == "image/png" else ".jpg"
    return Response(
        content,
        media_type=proof.media_type,
        headers={
            "Content-Disposition": f'attachment; filename="proof-{sha256[:12]}{suffix}"',
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, no-store",
        },
    )


@api_router.get("/api/v1/proof-objects")
async def download_presigned_proof(
    key: Annotated[str, Query(min_length=1, max_length=300)],
    expires: Annotated[int, Query(gt=0)],
    signature: Annotated[str, Query(pattern="^[0-9a-f]{64}$")],
) -> Response:
    """Redeem one short-lived GET capability without making the bucket public."""

    try:
        proof_link_signer.verify_get(
            key=key,
            expires=expires,
            signature=signature,
            now=datetime.now(UTC),
        )
    except InvalidProofLink as error:
        raise HTTPException(status_code=403, detail="invalid or expired proof link") from error
    stored = await proof_storage.get_private(key)
    if stored is None:
        raise HTTPException(status_code=404, detail="proof object not found")
    content, media_type = stored
    suffix = ".png" if media_type == "image/png" else ".jpg"
    digest = key.rsplit("/", 1)[-1].split(".", 1)[0]
    return Response(
        content,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="proof-{digest[:12]}{suffix}"',
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, no-store",
        },
    )


@api_router.get("/api/v1/exports/{order_id}")
async def export_order(
    order_id: UUID,
    service: Annotated[OrderService, Depends(get_order_service)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> StreamingResponse:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    async def rows():
        yield b"order_id,status\n"
        yield f"{order_id},{order.status.value}\n".encode("utf-8")
    return StreamingResponse(
        rows(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="order-{order_id}.csv"'},
    )


@api_router.get("/api/v1/orders/{order_id}/tracking/events")
async def sse_tracking(
    request: Request,
    order_id: UUID,
    service: Annotated[OrderService, Depends(get_order_service)],
    snapshots: Annotated[TrackingSnapshotRepository, Depends(get_tracking_snapshot_repository)],
    actor: Annotated[Actor, Depends(current_actor)],
    follow: bool = True,
) -> StreamingResponse:
    order = await service.get(order_id)
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        raise HTTPException(status_code=403, detail="order access denied")
    raw_position = request.headers.get("Last-Event-ID", "0")
    last_sequence = int(raw_position) if raw_position.isdecimal() else 0
    current = await snapshots.get(order.id)
    snapshot = current.event() if current is not None else TrackingEvent(
        order_id=order.id,
        status=order.status.value,
        sequence=0,
    )
    return StreamingResponse(
        order_event_stream(
            hub=request.app.state.tracking,
            snapshot=snapshot,
            last_sequence=last_sequence,
            follow=follow,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _wait_for_websocket_disconnect(websocket: WebSocket) -> None:
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return


async def _send_tracking_events(
    websocket: WebSocket, queue: asyncio.Queue[TrackingEvent]
) -> None:
    while True:
        event = await queue.get()
        await websocket.send_json(event.payload())


def _websocket_bearer_protocol(websocket: WebSocket) -> tuple[str, str] | None:
    """Read a bearer token from the negotiated protocol, never from the URL."""
    offered = [item.strip() for item in websocket.headers.get("sec-websocket-protocol", "").split(",")]
    protocol = next((item for item in offered if item.startswith("bearer.")), None)
    if protocol is None or len(protocol) <= len("bearer."):
        return None
    return protocol.removeprefix("bearer."), protocol


async def _load_tracking_snapshot(order_id: UUID):
    if get_settings().use_in_memory_repository:
        return await tracking_snapshots.get(order_id)
    async with transactional_session() as session:
        return await SqlAlchemyTrackingSnapshotRepository(session).get(order_id)


async def _publish_tracking_event(app: FastAPI, event: TrackingEvent) -> None:
    if get_settings().use_in_memory_repository:
        await app.state.tracking.publish(event)
        return
    await app.state.redis_client.publish(
        TRACKING_CHANNEL,
        json.dumps(event.payload(), ensure_ascii=False, separators=(",", ":")),
    )


async def _consume_tracking_bus(app: FastAPI) -> None:
    while True:
        try:
            async with app.state.redis_client.pubsub() as subscriber:
                await subscriber.subscribe(TRACKING_CHANNEL)
                async for message in subscriber.listen():
                    if message.get("type") != "message":
                        continue
                    raw = message.get("data")
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    await app.state.tracking.publish(
                        TrackingEvent.from_payload(json.loads(raw))
                    )
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError, ValueError, json.JSONDecodeError):
            # PostgreSQL snapshot remains authoritative while the transient
            # transport reconnects. Outbox replay restores later delivery.
            await asyncio.sleep(1)


@api_router.post(
    "/api/v1/courier/orders/{order_id}/tracking",
    response_model=TrackingRead,
    status_code=status.HTTP_201_CREATED,
)
async def record_tracking_position(
    request: Request,
    order_id: UUID,
    update: TrackingUpdate,
    background_tasks: BackgroundTasks,
    service: Annotated[OrderService, Depends(get_order_service)],
    snapshots: Annotated[TrackingSnapshotRepository, Depends(get_tracking_snapshot_repository)],
    actor: Annotated[Actor, Depends(current_actor)],
) -> TrackingRead:
    order = await service.get(order_id)
    if (
        actor.role is not Role.COURIER
        or actor.tenant_id != order.tenant_id
        or actor.user_id != order.courier_id
    ):
        raise HTTPException(status_code=403, detail="tracking access denied")
    if order.status not in {OrderStatus.ASSIGNED, OrderStatus.PICKED_UP}:
        raise HTTPException(status_code=409, detail="tracking is not active for this order")
    try:
        snapshot, changed = await snapshots.record(
            order_id=order.id,
            tenant_id=order.tenant_id,
            status=order.status.value,
            latitude=update.latitude,
            longitude=update.longitude,
            client_event_id=update.client_event_id,
            recorded_at=datetime.now(UTC),
        )
    except ValueError as error:
        raise HTTPException(status_code=409, detail="tracking event conflict") from error
    if changed:
        background_tasks.add_task(_publish_tracking_event, request.app, snapshot.event())
    return TrackingRead(
        order_id=snapshot.order_id,
        sequence=snapshot.sequence,
        latitude=snapshot.latitude,
        longitude=snapshot.longitude,
        status=snapshot.status,
        recorded_at=snapshot.recorded_at,
    )


@api_router.websocket("/api/v1/orders/{order_id}/tracking")
async def tracking(websocket: WebSocket, order_id: UUID) -> None:
    credentials = _websocket_bearer_protocol(websocket)
    if credentials is None:
        await websocket.close(code=4401)
        return
    token, selected_protocol = credentials
    try:
        actor = actor_from_claims(decode_token(token))
    except HTTPException:
        await websocket.close(code=4401)
        return
    service = get_order_service(repository)
    try:
        order = await service.get(order_id)
    except OrderNotFound:
        await websocket.close(code=4404)
        return
    if not owns_order(actor, tenant_id=order.tenant_id, customer_id=order.customer_id, courier_id=order.courier_id):
        await websocket.close(code=4403)
        return
    queue = websocket.app.state.tracking.subscribe(order_id)
    snapshot = await _load_tracking_snapshot(order_id)
    initial = snapshot.event() if snapshot is not None else TrackingEvent(
        order_id=order_id, status=order.status.value, sequence=0,
    )
    await websocket.accept(subprotocol=selected_protocol)
    await websocket.send_json(initial.payload())
    sender = asyncio.create_task(_send_tracking_events(websocket, queue))
    receiver = asyncio.create_task(_wait_for_websocket_disconnect(websocket))
    try:
        done, pending = await asyncio.wait(
            {sender, receiver}, return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    except (WebSocketDisconnect, OSError, asyncio.CancelledError):
        pass
    finally:
        sender.cancel()
        receiver.cancel()
        await asyncio.gather(sender, receiver, return_exceptions=True)
        websocket.app.state.tracking.unsubscribe(order_id, queue)


app.include_router(api_router)
