from __future__ import annotations

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DELIVERY_", env_file=".env", extra="ignore")

    environment: Literal["development", "test", "production"] = "development"
    use_in_memory_repository: bool = True
    database_url: str = "postgresql+asyncpg://delivery:delivery@postgres/delivery"
    database_pool_size: int = Field(default=8, gt=0, le=50)
    database_max_overflow: int = Field(default=4, ge=0, le=50)
    database_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    database_pool_recycle_seconds: int = Field(default=1800, gt=0, le=7200)
    redis_url: str = "redis://redis:6379/0"
    celery_broker_url: str = "redis://redis:6379/1"
    celery_visibility_timeout_seconds: int = Field(default=300, ge=60, le=86_400)
    celery_soft_time_limit_seconds: int = Field(default=45, ge=5, le=3_600)
    celery_hard_time_limit_seconds: int = Field(default=60, ge=10, le=3_900)
    redis_connect_timeout_seconds: float = Field(default=0.2, gt=0, le=2)
    redis_socket_timeout_seconds: float = Field(default=0.3, gt=0, le=2)
    redis_max_connections: int = Field(default=20, gt=0, le=100)
    delivery_zone_cache_ttl_seconds: int = Field(default=60, gt=0, le=3600)
    jwt_secret: SecretStr = SecretStr("local-development-only-change-me")
    jwt_issuer: str = "delivery-service"
    jwt_audience: str = "delivery-api"
    webhook_secret: SecretStr = SecretStr("local-webhook-secret-change-me-1234")
    webhook_tolerance_seconds: int = Field(default=300, ge=30, le=900)
    webhook_max_bytes: int = Field(default=64 * 1024, ge=1024, le=1024 * 1024)
    allowed_origins: tuple[str, ...] = ("http://localhost:3000",)
    allowed_hosts: tuple[str, ...] = ("localhost", "127.0.0.1", "testserver")
    max_upload_bytes: int = Field(default=5 * 1024 * 1024, gt=0, le=20 * 1024 * 1024)
    proof_link_secret: SecretStr = SecretStr(
        "local-proof-link-secret-change-me-123456"
    )
    proof_link_ttl_seconds: int = Field(default=300, ge=30, le=900)
    tracking_queue_size: int = Field(default=32, gt=0, le=256)
    route_service_url: str = "https://routes.example.test"
    route_api_key: SecretStr = SecretStr("local-route-key-change-me")
    route_connect_timeout_seconds: float = Field(default=1.0, gt=0, le=10)
    route_pool_timeout_seconds: float = Field(default=0.5, gt=0, le=10)
    route_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    route_max_connections: int = Field(default=50, gt=0, le=500)
    route_max_keepalive_connections: int = Field(default=20, ge=0, le=500)
    route_keepalive_expiry_seconds: float = Field(default=30.0, gt=0, le=300)
    otel_exporter_otlp_endpoint: str | None = None
    otel_sample_ratio: float = Field(default=1.0, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def production_requires_external_secret(self) -> Self:
        jwt_secret = self.jwt_secret.get_secret_value()
        webhook_secret = self.webhook_secret.get_secret_value()
        if self.environment == "production" and jwt_secret.startswith("local-development"):
            raise ValueError("DELIVERY_JWT_SECRET is required outside development")
        if self.environment == "production" and len(jwt_secret) < 32:
            raise ValueError("DELIVERY_JWT_SECRET must contain at least 32 characters")
        if self.environment == "production" and webhook_secret.startswith("local-webhook"):
            raise ValueError("DELIVERY_WEBHOOK_SECRET is required outside development")
        if self.environment == "production" and len(webhook_secret) < 32:
            raise ValueError("DELIVERY_WEBHOOK_SECRET must contain at least 32 characters")
        proof_link_secret = self.proof_link_secret.get_secret_value()
        if self.environment == "production" and proof_link_secret.startswith("local-proof"):
            raise ValueError("DELIVERY_PROOF_LINK_SECRET is required outside development")
        if self.environment == "production" and len(proof_link_secret) < 32:
            raise ValueError("DELIVERY_PROOF_LINK_SECRET must contain at least 32 characters")
        route_api_key = self.route_api_key.get_secret_value()
        if self.environment == "production" and route_api_key.startswith("local-route-key"):
            raise ValueError("DELIVERY_ROUTE_API_KEY is required outside development")
        if self.environment == "production" and len(route_api_key) < 24:
            raise ValueError("DELIVERY_ROUTE_API_KEY must contain at least 24 characters")
        if self.environment == "production":
            if not self.allowed_origins or "*" in self.allowed_origins:
                raise ValueError("DELIVERY_ALLOWED_ORIGINS must be an explicit production allowlist")
            if any(origin.startswith("http://localhost") for origin in self.allowed_origins):
                raise ValueError("localhost is not a production browser origin")
            if not self.allowed_hosts or "*" in self.allowed_hosts:
                raise ValueError("DELIVERY_ALLOWED_HOSTS must be an explicit production allowlist")
        if self.route_max_keepalive_connections > self.route_max_connections:
            raise ValueError(
                "DELIVERY_ROUTE_MAX_KEEPALIVE_CONNECTIONS cannot exceed "
                "DELIVERY_ROUTE_MAX_CONNECTIONS"
            )
        if self.celery_soft_time_limit_seconds >= self.celery_hard_time_limit_seconds:
            raise ValueError(
                "DELIVERY_CELERY_SOFT_TIME_LIMIT_SECONDS must be below the hard limit"
            )
        if self.celery_visibility_timeout_seconds <= self.celery_hard_time_limit_seconds:
            raise ValueError(
                "DELIVERY_CELERY_VISIBILITY_TIMEOUT_SECONDS must exceed the hard time limit"
            )
        if self.database_pool_size + self.database_max_overflow > 64:
            raise ValueError(
                "DELIVERY_DATABASE_POOL_SIZE plus DELIVERY_DATABASE_MAX_OVERFLOW "
                "cannot exceed 64 connections per worker"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
