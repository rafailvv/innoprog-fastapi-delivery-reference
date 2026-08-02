# Release and rollback runbook

This runbook changes only the selected immutable image digest. It never rebuilds
an old branch during an incident and never runs an automatic Alembic downgrade.

## Ownership, severity and evidence

- **Incident commander:** owns the timeline, mitigation decision and hand-off.
- **Application operator:** checks rollout, API, worker and outbox evidence.
- **Database operator:** checks revision, locks, saturation, backup and restore.
- **Security contact:** joins for authentication, authorization, secret or
  supply-chain signals.

Declare SEV-1 when order integrity, tenant isolation or authentication is
violated. Declare SEV-2 when availability or latency is outside the SLO without
confirmed data corruption. Record UTC timestamps, current/previous image
digests, Alembic revision and correlation IDs. Never copy tokens, passwords,
full addresses or coordinates into the incident channel.

## First five minutes

1. Acknowledge the alert, name the incident commander and freeze promotion.
2. Record blast radius: tenants, routes, workers and the first known UTC time.
3. Verify liveness and readiness separately:

   ```bash
   curl --fail https://api.delivery.example/health/live
   curl --fail https://api.delivery.example/health/ready
   ```

4. Compare `http_requests_total`, p95, saturation, oldest outbox age and current
   circuit state with the release baseline. Use route templates, never order IDs,
   as metric labels.
5. Check application logs by correlation ID and trace ID; identify the first
   failing boundary rather than restarting every component.

## Diagnostic queries

Run these read-only queries with a least-privileged incident role:

```sql
SELECT version_num FROM alembic_version;

SELECT count(*) AS pending,
       max(now() - created_at) AS oldest_age
FROM outbox
WHERE published_at IS NULL AND processed_at IS NULL;

SELECT state, wait_event_type, wait_event, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state, wait_event_type, wait_event;

SELECT status, count(*)
FROM delivery_order
GROUP BY status
ORDER BY status;
```

Growing outbox age with healthy API points to relay, broker or consumer. High
database lock waits point to transaction contention. Authentication failures
after a release require checking issuer, audience, clock and secret version;
do not disable signature verification as mitigation.

## Required release evidence

- approved commit SHA;
- candidate `image@sha256` from the signed release manifest;
- SBOM, provenance and successful Trivy result for that digest;
- current and target Alembic revisions;
- previous production digest and its compatible schema range;
- green unit, integration, migration, container and acceptance checks.

## Compatibility window

Revision `0004` is an expand migration. It preserves `outbox.processed_at`, adds
`outbox.published_at`, backfills it and lets the new relay dual-read and
dual-write both columns. The previous image and the candidate are therefore
compatible with revisions `0005` and `0006`.

Do not remove `processed_at` while the previous image remains the rollback
candidate. That destructive contract migration is a separate future release.

## Release procedure

1. Record the current production digest and Alembic revision.
2. Verify the candidate manifest signature and exact commit/digest/revision.
3. Run the single migration job. It acquires the PostgreSQL advisory lock.
4. Start the candidate without traffic and wait for readiness.
5. Send a bounded canary share and observe availability, p95, saturation,
   outbox lag and business invariant checks.
6. Promote the same digest only while every gate stays within its threshold.
7. Record the final digest, revision, timestamps and operator.

Before promotion run the public black-box acceptance probe from a clean
operator environment. Supply short-lived customer and foreign-user tokens;
the probe must prove readiness, metrics, idempotent replay, ownership denial,
optimistic conflict, state transition and export:

```bash
python scripts/acceptance_smoke.py \
  --base-url https://candidate.delivery.example \
  --customer-id "$CUSTOMER_ID" \
  --customer-token "$CUSTOMER_TOKEN" \
  --foreign-token "$FOREIGN_TOKEN"
```

Then run the bounded open-loop capacity smoke. Promotion requires error rate
at most 1%, end-to-end p95 at most 500 ms, stable database pool saturation and
no growth of the oldest pending outbox event.

## Rollback criteria

Rollback when any pre-agreed gate persists beyond its evaluation window:

- readiness does not become healthy;
- server-error rate or p95 exceeds the release threshold;
- PostgreSQL errors, outbox lag or queue saturation grows;
- authentication, authorization or data integrity regresses;
- a critical secret or supply-chain incident affects the candidate.

## Rollback procedure

1. Stop promotion and preserve logs, metrics, traces and the release manifest.
2. Confirm the recorded previous digest supports the current schema revision.
3. Change deployment from the candidate digest to the previous digest.
4. Wait for previous instances to become ready before removing the candidate.
5. Run order create/read, authentication and outbox acceptance probes.
6. Confirm error rate, p95, saturation and outbox lag recover.
7. Record the incident and decide between a forward fix and a later contract
   migration. Do not execute `alembic downgrade` as an automatic response.

If the schema compatibility check fails, stop: the previous image is not a
valid rollback target. Use the documented forward-fix or recovery procedure
instead of deleting data to make the old binary start.

## Verification and reconciliation

Rollback is not complete when containers merely become healthy. Repeat the
black-box acceptance probe and compare metrics with the pre-incident baseline.
Then reconcile durable asynchronous work:

```sql
SELECT id, event_type, created_at
FROM outbox
WHERE published_at IS NULL AND processed_at IS NULL
ORDER BY created_at, id
LIMIT 100;

SELECT event_type, count(*)
FROM consumer_inbox
GROUP BY event_type
ORDER BY event_type;
```

Do not mark pending events manually without proving the external effect. Replay
through the normal idempotent consumer path. Check created orders without an
active assignment, tracking snapshots whose sequence stopped advancing and
webhook conflicts. Record every repair command and resulting row count.

## Backup restore and disaster recovery drill

The production policy defines **RPO 15 minutes** and **RTO 60 minutes** for this
course service. A backup is not accepted because a file exists; a scheduled
restore drill must prove it can produce a usable database.

1. Create an isolated PostgreSQL instance with no production network route.
2. Restore the selected base backup plus WAL to the recorded recovery point.
3. Verify checksums and the exact Alembic revision before starting application code.
4. Start the compatible immutable image without public traffic.
5. Run the black-box acceptance probe and read-only reconciliation queries.
6. Compare order, transition history, outbox, inbox and tracking counts with the
   backup manifest. Explain any difference within the declared RPO.
7. Measure elapsed restore time against RTO and store the drill evidence.
8. Destroy the isolated copy using the approved data-retention procedure.

Never test a restore by overwriting the only production database. Never expose
the restored copy through the public proxy or reuse production access tokens.

## Closing the incident

Close only after user-visible service is stable, the acceptance probe is green,
outbox lag is decreasing, security checks are unchanged and reconciliation has
no unexplained rows. Attach the timeline, root cause, affected invariants,
mitigation, rollback/forward-fix decision and follow-up owners with deadlines.
