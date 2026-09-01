# delivery-service

Учебный, но production-oriented сервис доставки для курса FastAPI. Он содержит
API заказов, назначение курьера, PostgreSQL migrations, Redis/Celery, signed
webhook, live tracking, audit, observability и проверяемый rollback runbook.

## Локальный запуск

Verifier читает точку входа из `pyproject.toml`:

```toml
[tool.innoprog]
asgi-app = "delivery_service.main:app"
```

Имя Python-пакета может быть другим; важно, чтобы import string указывал на реальный ASGI-объект.

```bash
docker compose up --build
```

После запуска доступны `/health/live`, `/health/ready`, OpenAPI и `/api/v1/orders`.
Команда сначала применяет Alembic migrations, затем запускает API и Celery worker;
API использует PostgreSQL repository, а in-memory adapter остаётся только быстрым тестовым fixture.

HTML-карточка `/pages/orders/{order_id}` рендерится через Jinja2 только после
authentication и ownership-проверки. В template context передаётся отдельная
allowlisted view model; autoescape остаётся включённым.

Multipart upload подтверждения использует bounded `UploadFile`: Nginx задаёт
`client_max_body_size`, приложение отличает `payload_too_large` от неверного
формата и не доверяет клиентскому MIME type. Выдача объекта и CSV streaming
использует `Content-Disposition`, `nosniff`, authentication и ownership.

## Границы и эксплуатация

- `delivery`, `courier`, `identity` и `tracking` развиваются как package-by-feature.
- Повтор создания защищён заголовком idempotency, а события сохраняются через outbox.
- Назначение курьера использует transaction, unit_of_work, lock и optimistic version.
- Redis cache использует versioned keys, явный TTL и post-commit invalidation;
  cache miss, eviction и недоступность Redis ведут к PostgreSQL fallback.
- Redis lease имеет конечный TTL и owner token с compare-and-delete. Она
  уменьшает дубли восстановимой работы, но не заменяет constraint, conditional
  write или идемпотентный consumer для устойчивого бизнес-эффекта.
- Authorization проверяет access/refresh JWT, RBAC и ownership каждого заказа;
  роли customer, courier и dispatcher имеют разные разрешения.
- Rate limit возвращает 429 и оставляет audit event без секретов.
- Метрики покрывают RED signals; tracing построен на OpenTelemetry.
- При инциденте сначала проверяются readiness, error rate и migrations, затем
  выполняется rollback приложения на совместимую версию по
  [`docs/release-and-rollback.md`](docs/release-and-rollback.md). Acceptance
  suite подтверждает основной сценарий до направления трафика.

## Тестовые доказательства

Unit suite использует fake repository, API suite — `ASGITransport` и
`dependency_overrides`, а интеграционный suite — PostgreSQL/Testcontainers.
Отдельный concurrent race test одновременно назначает одного курьера и
подтверждает, что ограничение и блокировка сохраняют инвариант.

## Карта эксплуатационных гарантий

- Transaction isolation выбирается на границе use case; serialization failure и
  deadlock повторяют транзакцию целиком, а не отдельный SQL statement.
- Optimistic version обнаруживает устаревшую запись, pessimistic lock и
  PostgreSQL advisory lock защищают короткие конкурентные секции.
- Redis cache хранит только восстанавливаемые данные с TTL. Недоступность cache
  не меняет корректность PostgreSQL-состояния.
- Пароли хешируются Argon2; OAuth2 access/refresh JWT проверяют issuer,
  audience, expiration и тип token. RBAC дополняется ownership-проверкой.
- Cookie-аутентификация использует CSRF token. Rate limiting возвращает 429,
  а security audit фиксирует outcome без токена и пароля.
- Celery consumer и transactional outbox переносят повторную доставку. Webhook
  проверяет HMAC signature и replay, объектное storage выдаёт короткий presigned URL.
- Prometheus metrics описывают RED signals и SLO; OpenTelemetry trace переносит
  контекст. Load test контролирует p95 и resource capacity.
- State machine разрешает только `created → assigned/cancelled`,
  `assigned → picked_up/cancelled` и `picked_up → delivered`. Каждый принятый
  переход одной транзакцией меняет optimistic version, добавляет immutable
  `order_transition_history` и создаёт `order.status_changed` в outbox;
  запрещённая или устаревшая команда не оставляет этих эффектов.
- Live tracking и backpressure проверяются acceptance suite. Runbook содержит
  diagnosis, disaster recovery и rollback совместимой версии.

## Финальная приёмка выпуска

Перед направлением production-трафика оператор запускает публичный black-box
probe [`scripts/acceptance_smoke.py`](scripts/acceptance_smoke.py). Он проверяет
liveness/readiness, metrics, идемпотентный replay, запрет чужого заказа,
optimistic conflict, допустимый переход состояния, фильтр списка и export, не
обращаясь напрямую к repository или базе данных.

Полный порядок release, rollback, forward fix, outbox reconciliation и restore
drill описан в
[`docs/release-and-rollback.md`](docs/release-and-rollback.md). Для учебного
production-контура зафиксированы RPO 15 минут и RTO 60 минут; эти числа
считаются доказанными только после изолированного восстановления backup и
повторного acceptance-сценария.
