import pytest
from pydantic import ValidationError

from delivery_service.config import Settings


def test_environment_value_is_parsed(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_ROUTE_TIMEOUT_SECONDS", "3.5")
    settings = Settings()
    assert settings.route_timeout_seconds == 3.5


def test_route_pool_rejects_more_idle_than_total_connections() -> None:
    with pytest.raises(ValidationError, match="MAX_KEEPALIVE_CONNECTIONS"):
        Settings(route_max_connections=10, route_max_keepalive_connections=11)


def test_database_pool_rejects_unbounded_per_worker_budget() -> None:
    with pytest.raises(ValidationError, match="cannot exceed 64"):
        Settings(database_pool_size=40, database_max_overflow=30)


def test_development_secret_is_rejected_in_production(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_ENVIRONMENT", "production")
    with pytest.raises(ValidationError, match="DELIVERY_JWT_SECRET"):
        Settings()


def test_secret_is_absent_from_repr() -> None:
    raw = "a-production-secret-with-more-than-32-characters"
    settings = Settings(jwt_secret=raw, route_api_key=raw)
    assert raw not in repr(settings)
    assert settings.jwt_secret.get_secret_value() == raw
    assert settings.route_api_key.get_secret_value() == raw


def test_unknown_environment_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(environment="prodution")


def test_production_rejects_wildcard_edge_allowlists() -> None:
    secret = "a-production-secret-with-more-than-32-characters"
    with pytest.raises(ValidationError, match="DELIVERY_ALLOWED_ORIGINS"):
        Settings(
            environment="production",
            jwt_secret=secret,
            webhook_secret=secret,
            route_api_key=secret,
            proof_link_secret=secret,
            allowed_origins=("*",),
            allowed_hosts=("api.delivery.example",),
        )

    with pytest.raises(ValidationError, match="DELIVERY_ALLOWED_HOSTS"):
        Settings(
            environment="production",
            jwt_secret=secret,
            webhook_secret=secret,
            route_api_key=secret,
            proof_link_secret=secret,
            allowed_origins=("https://app.delivery.example",),
            allowed_hosts=("*",),
        )


def test_production_rejects_default_route_api_key() -> None:
    secret = "a-production-secret-with-more-than-32-characters"
    with pytest.raises(ValidationError, match="DELIVERY_ROUTE_API_KEY"):
        Settings(
            environment="production",
            jwt_secret=secret,
            webhook_secret=secret,
            proof_link_secret=secret,
            allowed_origins=("https://app.delivery.example",),
            allowed_hosts=("api.delivery.example",),
        )


def test_production_accepts_explicit_edge_allowlists() -> None:
    secret = "a-production-secret-with-more-than-32-characters"
    settings = Settings(
        environment="production",
        jwt_secret=secret,
        webhook_secret=secret,
        route_api_key=secret,
        proof_link_secret=secret,
        allowed_origins=("https://app.delivery.example",),
        allowed_hosts=("api.delivery.example",),
    )

    assert settings.allowed_origins == ("https://app.delivery.example",)
    assert settings.allowed_hosts == ("api.delivery.example",)
