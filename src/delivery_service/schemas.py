from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from delivery_service.domain import OrderStatus


Address = Annotated[
    str,
    Field(
        min_length=5,
        max_length=300,
        description="Адрес одной точки доставки",
        examples=["Невский проспект, 1"],
    ),
]
WeightGrams = Annotated[
    int,
    Field(
        gt=0,
        le=100_000,
        strict=True,
        description="Вес отправления в граммах",
        examples=[750],
    ),
]


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: UUID
    pickup_address: Address
    destination_address: Address
    weight_grams: WeightGrams

    @field_validator("pickup_address", "destination_address", mode="before")
    @classmethod
    def normalize_address(cls, value: Any) -> Any:
        return " ".join(value.split()) if isinstance(value, str) else value

    @model_validator(mode="after")
    def addresses_must_differ(self) -> "OrderCreate":
        if self.pickup_address.casefold() == self.destination_address.casefold():
            raise ValueError("pickup and destination addresses must differ")
        return self


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    customer_id: UUID
    pickup_address: str
    destination_address: str
    weight_grams: int
    status: OrderStatus
    courier_id: UUID | None
    version: int
    created_at: datetime


class OrderUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pickup_address: Address | None = None
    destination_address: Address | None = None
    weight_grams: WeightGrams | None = None

    @field_validator("pickup_address", "destination_address", mode="before")
    @classmethod
    def normalize_address(cls, value: Any) -> Any:
        return " ".join(value.split()) if isinstance(value, str) else value


class DeliveryWindow(BaseModel):
    starts_at: datetime
    ends_at: datetime

    @model_validator(mode="after")
    def valid_duration(self) -> Self:
        duration = self.ends_at - self.starts_at
        if duration <= timedelta(0):
            raise ValueError("ends_at must be later than starts_at")
        if duration > timedelta(hours=4):
            raise ValueError("delivery window must not exceed four hours")
        return self


class OrderTransition(BaseModel):
    status: OrderStatus


class TrackingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_event_id: UUID
    latitude: Annotated[float, Field(ge=-90, le=90)]
    longitude: Annotated[float, Field(ge=-180, le=180)]
    accuracy_meters: Annotated[float | None, Field(gt=0, le=1000)] = None


class TrackingRead(BaseModel):
    order_id: UUID
    sequence: int = Field(gt=0)
    latitude: float
    longitude: float
    status: str
    recorded_at: datetime


class Registration(BaseModel):
    subject: str = Field(pattern=r"^customer:[0-9a-fA-F-]{36}$")
    password: str = Field(min_length=12, max_length=256)


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int = Field(gt=0)


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    correlation_id: str
    details: list[dict[str, object]] = Field(default_factory=list)
