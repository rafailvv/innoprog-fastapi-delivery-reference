from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def test_runtime_image_is_multi_stage_non_root_and_signal_safe() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert len(re.findall(r"(?im)^FROM\s+\S+\s+AS\s+\w+", dockerfile)) >= 2
    assert "COPY --from=builder" in dockerfile
    assert re.search(r"(?im)^USER\s+10001:10001\s*$", dockerfile)
    assert re.search(r'(?im)^HEALTHCHECK\s+.*?/health/live', dockerfile)
    assert re.search(r'(?im)^CMD\s*\["python",\s*"-m",\s*"uvicorn"', dockerfile)
    assert not re.search(r"(?im)^CMD\s+(?!\[)", dockerfile)


def test_build_context_excludes_local_state_and_secrets() -> None:
    ignored = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {".git", ".venv", ".env", "__pycache__", ".pytest_cache"} <= ignored


def test_compose_bounds_api_and_keeps_dependencies_internal() -> None:
    compose = (ROOT / "compose.yml").read_text(encoding="utf-8")
    api = compose.split("\n  api:\n", 1)[1].split("\n  worker:\n", 1)[0]
    migrate = compose.split("\n  migrate:\n", 1)[1].split("\n  postgres:\n", 1)[0]
    postgres = compose.split("\n  postgres:\n", 1)[1].split("\n  redis:\n", 1)[0]
    redis = compose.split("\n  redis:\n", 1)[1].split("\nvolumes:\n", 1)[0]

    for token in (
        'user: "10001:10001"', "init: true", "read_only: true",
        "cap_drop: [ALL]", 'security_opt: ["no-new-privileges:true"]',
        "tmpfs:", "stop_grace_period: 30s", "mem_limit: 512m",
        "cpus: 1.0", "pids_limit: 128", "/health/ready",
    ):
        assert token in api
    assert 'command: ["alembic", "upgrade", "head"]' in migrate
    assert "internal: true" in compose
    assert "ports:" not in postgres
    assert "ports:" not in redis


def test_liveness_and_readiness_are_not_collapsed() -> None:
    source = (ROOT / "src/delivery_service/main.py").read_text(encoding="utf-8")

    assert '@api_router.get("/health/live"' in source
    assert '@api_router.get("/health/ready"' in source
    assert "app.state.ready = True" in source
    assert source.count("app.state.ready = False") >= 2
    assert "await app.state.http_client.aclose()" in source
    assert "await app.state.redis_client.aclose()" in source
    assert "await engine.dispose()" in source
