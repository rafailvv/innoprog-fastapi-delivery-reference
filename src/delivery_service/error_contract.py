from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from fastapi import Request
from fastapi.responses import JSONResponse

from delivery_service.observability import correlation_id


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    """Stable public description of one API failure class."""

    status_code: int
    error_code: str
    message: str


REQUEST_INVALID = ErrorSpec(422, "request_invalid", "Request validation failed")
ORDER_NOT_FOUND = ErrorSpec(404, "order_not_found", "Order was not found")
INVALID_PROOF = ErrorSpec(415, "invalid_proof", "Proof file is not supported")
UPLOAD_TOO_LARGE = ErrorSpec(413, "payload_too_large", "Proof file exceeds the size limit")
CONCURRENT_UPDATE = ErrorSpec(409, "concurrent_update", "Order was changed by another request")
COURIER_ALREADY_BUSY = ErrorSpec(
    409, "courier_already_busy", "Courier already has an active assignment"
)
IDEMPOTENCY_CONFLICT = ErrorSpec(
    409,
    "idempotency_conflict",
    "Idempotency-Key was already used for another request",
)
INVALID_TRANSITION = ErrorSpec(409, "invalid_transition", "Order transition is not allowed")
INTERNAL_ERROR = ErrorSpec(500, "internal_error", "The service could not complete the request")


HTTP_ERROR_SPECS: dict[int, ErrorSpec] = {
    400: ErrorSpec(400, "request_invalid", "Request could not be understood"),
    401: ErrorSpec(401, "authentication_required", "Authentication is required"),
    403: ErrorSpec(403, "access_denied", "Access is denied"),
    404: ErrorSpec(404, "resource_not_found", "Resource was not found"),
    405: ErrorSpec(405, "method_not_allowed", "Method is not allowed for this resource"),
    409: ErrorSpec(409, "request_conflict", "Request conflicts with current state"),
    413: ErrorSpec(413, "payload_too_large", "Request payload is too large"),
    429: ErrorSpec(429, "rate_limited", "Too many requests"),
    503: ErrorSpec(503, "service_unavailable", "Service is temporarily unavailable"),
}


def error_response(
    request: Request,
    spec: ErrorSpec,
    *,
    details: Sequence[Mapping[str, object]] = (),
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Build every public API error from the same allowlisted fields."""

    request_id = correlation_id.get() or getattr(request.state, "correlation_id", "")
    response_headers = {**dict(headers or {}), "X-Request-ID": request_id}
    return JSONResponse(
        status_code=spec.status_code,
        content={
            "error_code": spec.error_code,
            "message": spec.message,
            "correlation_id": request_id,
            "details": [dict(item) for item in details],
        },
        headers=response_headers,
    )
