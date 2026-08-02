from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from delivery_service.db import OutboxRow
from delivery_service.outbox import (
    mark_outbox_published,
    transitional_outbox_pending_clause,
)
from delivery_service.release import verify_release_manifest


ROOT = Path(__file__).resolve().parents[1]
COMMIT = "a" * 40
DIGEST_IMAGE = "ghcr.io/example/delivery-service@sha256:" + "b" * 64


def _manifest(**changes: str) -> str:
    payload = {"commit": COMMIT, "image": DIGEST_IMAGE, "alembic": "0007"}
    payload.update(changes)
    return json.dumps(payload)


def test_release_manifest_binds_commit_digest_and_schema_window() -> None:
    manifest = verify_release_manifest(
        _manifest(),
        expected_commit=COMMIT,
        compatible_revisions=frozenset({"0006", "0007"}),
    )
    assert manifest.image == DIGEST_IMAGE

    invalid = (
        _manifest(image="ghcr.io/example/delivery-service:latest"),
        _manifest(commit="c" * 40),
        _manifest(alembic="0004"),
    )
    for raw in invalid:
        with pytest.raises(ValueError):
            verify_release_manifest(
                raw,
                expected_commit=COMMIT,
                compatible_revisions=frozenset({"0006", "0007"}),
            )


def test_expand_window_dual_reads_and_dual_writes_outbox_markers() -> None:
    statement = select(OutboxRow.id).where(transitional_outbox_pending_clause())
    sql = str(statement.compile(dialect=postgresql.dialect())).casefold()
    assert "published_at is null" in sql
    assert "processed_at is null" in sql

    moment = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    row = OutboxRow(event_type="order.created", payload={"schema_version": 1})
    mark_outbox_published(row, moment)
    assert row.published_at == moment
    assert row.legacy_processed_at == moment


def test_release_files_encode_separate_migration_and_rollback_contract() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    compose = (ROOT / "compose.yml").read_text(encoding="utf-8")
    production_nginx = (ROOT / "nginx.production.conf").read_text(encoding="utf-8")
    runbook = (ROOT / "docs/release-and-rollback.md").read_text(encoding="utf-8").casefold()
    migration = (ROOT / "alembic/versions/0004_outbox_relay.py").read_text(encoding="utf-8")
    migration_env = (ROOT / "alembic/env.py").read_text(encoding="utf-8")

    assert "steps.build.outputs.digest" in workflow
    assert "provenance: mode=max" in workflow and "sbom: true" in workflow
    assert "trivy-action" in workflow and "release-manifest.json" in workflow
    assert "service_completed_successfully" in compose
    assert 'command: ["alembic", "upgrade", "head"]' in compose
    assert "listen 443 ssl" in production_nginx
    assert "ssl_certificate /run/secrets/" in production_nginx
    assert "proxy_set_header X-Forwarded-For $remote_addr" in production_nginx
    assert "$proxy_add_x_forwarded_for" not in production_nginx
    assert "previous digest" in runbook and "alembic downgrade" in runbook
    assert "compatibility window" in runbook and "forward fix" in runbook
    assert 'op.add_column(\n        "outbox"' in migration
    assert "processed_at" in migration and "published_at" in migration
    assert "new_column_name" not in migration
    assert "pg_advisory_xact_lock" in migration_env
    assert "pg_advisory_lock(" not in migration_env
