from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_release_probe_is_public_black_box_and_covers_negative_paths() -> None:
    source = (ROOT / "scripts" / "acceptance_smoke.py").read_text("utf-8")

    assert all(token in source for token in (
        "/health/live",
        "/health/ready",
        "/metrics",
        "/api/v1/orders",
        "Idempotency-Key",
        "Authorization",
        "X-Expected-Version",
        "foreign_access_denied",
        "stale_update_rejected",
    ))
    assert all(shortcut not in source for shortcut in (
        "delivery_service.repository",
        "delivery_service.db",
        "AsyncSession",
        "sessionmaker",
    ))


def test_runbook_contains_executable_diagnosis_recovery_and_restore_evidence() -> None:
    runbook = (ROOT / "docs" / "release-and-rollback.md").read_text("utf-8").casefold()

    assert all(section in runbook for section in (
        "## first five minutes",
        "## diagnostic queries",
        "## release procedure",
        "## rollback procedure",
        "## verification and reconciliation",
        "## backup restore and disaster recovery drill",
        "## closing the incident",
    ))
    assert all(evidence in runbook for evidence in (
        "alembic_version",
        "pg_stat_activity",
        "outbox",
        "consumer_inbox",
        "image@sha256",
        "forward fix",
        "rpo 15 minutes",
        "rto 60 minutes",
        "scripts/acceptance_smoke.py",
    ))
    assert "alembic downgrade" in runbook
    assert "do not execute" in runbook
