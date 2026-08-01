from __future__ import annotations

from dataclasses import dataclass

from delivery_service.domain import DeliveryOrder, OrderStatus


STATUS_LABELS = {
    OrderStatus.CREATED: "Создан",
    OrderStatus.ASSIGNED: "Назначен курьер",
    OrderStatus.PICKED_UP: "Передан курьеру",
    OrderStatus.DELIVERED: "Доставлен",
    OrderStatus.CANCELLED: "Отменён",
}


@dataclass(frozen=True, slots=True)
class OrderCardView:
    """Allowlisted, already formatted data for the server-rendered order card."""

    id: str
    public_number: str
    status_code: str
    status_label: str
    pickup_address: str
    destination_address: str
    weight_text: str
    created_at_text: str

    @classmethod
    def from_order(cls, order: DeliveryOrder) -> "OrderCardView":
        return cls(
            id=str(order.id),
            public_number=str(order.id).split("-", 1)[0].upper(),
            status_code=order.status.value,
            status_label=STATUS_LABELS[order.status],
            pickup_address=order.pickup_address,
            destination_address=order.destination_address,
            weight_text=f"{order.weight_grams / 1000:g} кг",
            created_at_text=order.created_at.strftime("%d.%m.%Y %H:%M UTC"),
        )
