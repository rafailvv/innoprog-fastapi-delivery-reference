from __future__ import annotations

import argparse
import asyncio
import json
from uuid import uuid4

import httpx

from delivery_service.performance import LoadPlan, OperationSpec, run_open_loop


def delivery_workload(order_id: str, customer_id: str, token: str) -> tuple[OperationSpec, ...]:
    auth = lambda _index: {"Authorization": f"Bearer {token}"}
    return (
        OperationSpec(
            "list_orders", "GET", "/api/v1/orders?limit=20", 50, frozenset({200}),
            header_factory=auth,
        ),
        OperationSpec(
            "get_order", "GET", f"/api/v1/orders/{order_id}", 25, frozenset({200}),
            header_factory=auth,
        ),
        OperationSpec(
            "tracking", "GET", f"/api/v1/orders/{order_id}/tracking/events", 15,
            frozenset({200}), header_factory=auth,
        ),
        OperationSpec(
            "create_order", "POST", "/api/v1/orders", 10, frozenset({201}),
            json_factory=lambda index: {
                "customer_id": customer_id,
                "pickup_address": "Load fixture A",
                "destination_address": "Load fixture B",
                "weight_grams": 1000 + index % 50,
            },
            header_factory=lambda _index: {
                "Authorization": f"Bearer {token}",
                "Idempotency-Key": f"load-{uuid4()}",
            },
        ),
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded open-loop delivery-service load smoke")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--customer-id", required=True)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()

    plan = LoadPlan(
        duration_seconds=args.duration,
        arrival_rate_per_second=args.rate,
        concurrency_limit=args.concurrency,
        timeout_seconds=3.0,
        operations=delivery_workload(args.order_id, args.customer_id, args.token),
    )
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=10)
    async with httpx.AsyncClient(base_url=args.base_url, limits=limits) as client:
        report = await run_open_loop(client, plan)
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    if report.error_rate > 0.01 or report.end_to_end_p95_ms > 500:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
