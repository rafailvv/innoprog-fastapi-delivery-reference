"""Black-box release probe for the public delivery-service HTTP contract."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

import httpx


class HttpClient(Protocol):
    def get(self, url: str, **kwargs) -> httpx.Response: ...
    def post(self, url: str, **kwargs) -> httpx.Response: ...
    def patch(self, url: str, **kwargs) -> httpx.Response: ...


@dataclass(frozen=True, slots=True)
class AcceptanceEvidence:
    order_id: str
    replayed: bool
    foreign_access_denied: bool
    stale_update_rejected: bool
    final_status: str
    metrics_exposed: bool

    def as_dict(self) -> dict[str, str | bool]:
        return {
            "order_id": self.order_id,
            "replayed": self.replayed,
            "foreign_access_denied": self.foreign_access_denied,
            "stale_update_rejected": self.stale_update_rejected,
            "final_status": self.final_status,
            "metrics_exposed": self.metrics_exposed,
        }


def require(response: httpx.Response, status: int, step: str) -> httpx.Response:
    if response.status_code != status:
        body = response.text[:500]
        raise RuntimeError(f"{step}: expected {status}, got {response.status_code}: {body}")
    return response


def run_acceptance(
    client: HttpClient,
    *,
    customer_id: str,
    customer_token: str,
    foreign_token: str,
    key: str,
) -> AcceptanceEvidence:
    """Exercise only public HTTP boundaries and return machine-readable evidence."""

    require(client.get("/health/live"), 200, "liveness")
    require(client.get("/health/ready"), 200, "readiness")
    require(client.get("/metrics"), 200, "metrics")

    headers = {
        "Authorization": f"Bearer {customer_token}",
        "Idempotency-Key": key,
        "X-Request-ID": f"acceptance-{key}",
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Acceptance street 1",
        "destination_address": "Release avenue 2",
        "weight_grams": 750,
    }
    created_response = require(
        client.post("/api/v1/orders", headers=headers, json=payload),
        201,
        "create order",
    )
    created = created_response.json()
    location = created_response.headers.get("Location")
    if not location:
        raise RuntimeError("create order: Location header is missing")

    replay = require(
        client.post("/api/v1/orders", headers=headers, json=payload),
        201,
        "idempotent replay",
    ).json()
    if replay != created:
        raise RuntimeError("idempotent replay returned another response snapshot")

    foreign = client.get(
        location,
        headers={"Authorization": f"Bearer {foreign_token}"},
    )
    if foreign.status_code not in {403, 404}:
        raise RuntimeError(f"foreign access was not denied: {foreign.status_code}")

    updated = require(
        client.patch(
            location,
            headers={**headers, "X-Expected-Version": "1"},
            json={"weight_grams": 800},
        ),
        200,
        "optimistic update",
    ).json()
    stale = client.patch(
        location,
        headers={**headers, "X-Expected-Version": "1"},
        json={"weight_grams": 900},
    )
    if stale.status_code != 409:
        raise RuntimeError(f"stale update was not rejected: {stale.status_code}")

    cancelled = require(
        client.post(
            f"{location}/transitions",
            headers={**headers, "X-Expected-Version": str(updated["version"])},
            json={"status": "cancelled"},
        ),
        200,
        "cancel order",
    ).json()
    visible = require(
        client.get("/api/v1/orders?status=cancelled", headers=headers),
        200,
        "filtered order list",
    ).json()
    if [item["id"] for item in visible] != [created["id"]]:
        raise RuntimeError("cancelled order is absent from the owner-visible page")
    export = require(
        client.get(f"/api/v1/exports/{created['id']}", headers=headers),
        200,
        "order export",
    )
    if created["id"] not in export.text:
        raise RuntimeError("order export does not contain the accepted order")
    metrics = require(client.get("/metrics"), 200, "metrics after scenario")

    return AcceptanceEvidence(
        order_id=created["id"],
        replayed=True,
        foreign_access_denied=True,
        stale_update_rejected=True,
        final_status=cancelled["status"],
        metrics_exposed="http_requests_total" in metrics.text,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="delivery-service release acceptance")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--customer-id", required=True)
    parser.add_argument("--customer-token", required=True)
    parser.add_argument("--foreign-token", required=True)
    parser.add_argument("--idempotency-key", default=f"release-{uuid4()}")
    args = parser.parse_args()
    with httpx.Client(base_url=args.base_url, timeout=5, trust_env=False) as client:
        evidence = run_acceptance(
            client,
            customer_id=args.customer_id,
            customer_token=args.customer_token,
            foreign_token=args.foreign_token,
            key=args.idempotency_key,
        )
    print(json.dumps(evidence.as_dict(), sort_keys=True))


if __name__ == "__main__":
    main()
