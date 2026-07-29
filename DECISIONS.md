# Technical Design Decisions & Concurrency Defense

This document details the core architectural decisions, concurrency guarantees, and lifecycle paths of `queuectl`.

---

### Q1: Concurrency and Race Prevention
*   **Exact Code Reference**: The update query inside [claim_next_job](file:///c:/Users/biswa/OneDrive/Desktop/QueueCTL/queuectl/db.py#L94-L125) in `queuectl/db.py`:
    ```sql
    UPDATE jobs
    SET state = 'processing',
        updated_at = ?,
        lease_expires_at = ?,
        attempts = attempts + 1,
        locked_by = ?,
        locked_at = ?
    WHERE id = (
        SELECT id FROM jobs
        WHERE (state = 'pending' AND (run_at IS NULL OR datetime(run_at) <= datetime(?)))
           OR (state = 'failed' AND (next_attempt_at IS NULL OR datetime(next_attempt_at) <= datetime(?)))
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
    ) AND state IN ('pending', 'failed')
    RETURNING id, command, state, attempts, max_retries, created_at, updated_at, locked_by, locked_at, lease_expires_at, next_attempt_at, priority, run_at, timeout_seconds, output;
    ```
*   **Why it is atomic**: 
    SQLite executes all writes sequentially by acquiring an exclusive write lock on the database file. The subquery `(SELECT id FROM jobs ... LIMIT 1)` runs inside the lock and returns a candidate ID. The outer `WHERE id = ... AND state IN ('pending', 'failed')` acts as a Compare-and-Swap constraint. 
    If two workers query simultaneously, SQLite serializes them. Worker 1 obtains the write lock, updates the selected job's state to `'processing'`, and commits. When Worker 2's turn comes, it attempts to match the selected ID. However, because the state of that row was updated to `'processing'` by Worker 1, Worker 2's outer `WHERE` clause fails to match, resulting in `0` rows updated (returns `None` and immediately releases lock via `conn.rollback()`).

---

### Q2: SIGKILL Crash Recovery Walkthrough
*   **Walkthrough of Events**:
    1.  **Claim Time**: A worker claims a job. Inside `claim_next_job`, `lease_expires_at` is set to `now + lease_duration` (default 15 seconds). State transitions to `'processing'`.
    2.  **Heartbeats**: During execution, the worker's background `LeaseRenewer` daemon thread updates `lease_expires_at` to `now + 15s` every 5 seconds.
    3.  **Crash (SIGKILL)**: The worker is killed instantly. The `LeaseRenewer` thread terminates immediately, freezing `lease_expires_at` at its last value.
    4.  **Reaper Sweep**: Another active worker starts a claim cycle and calls `reap_expired_jobs(conn)`. This performs a sweep query:
        ```sql
        SELECT id, attempts, max_retries FROM jobs WHERE state = 'processing' AND datetime(lease_expires_at) <= datetime(?);
        ```
    5.  **Recovery**: Once the frozen lease expires, `reap_expired_jobs` catches the job.
        *   If `attempts < max_retries`, state is reset to `'failed'`, and `next_attempt_at` is scheduled with exponential backoff: `now + (backoff_base ** attempts)` seconds.
        *   If `attempts >= max_retries`, state transitions to `'dead'`.
*   **Worst-Case Recovery Delay**: 
    *   `15s` (lease duration, if killed immediately after renewal) + `1s` (worker loop poll interval) = **16 seconds**. This is far below the 60-second requirement.

---

### Q3: DLQ Retry Attempts Policy
*   **Policy**: Manually retrying a job via `queuectl dlq retry <id>` resets `attempts` to `0`.
*   **Justification**:
    A DLQ retry is an operator-triggered event indicating that the underlying cause of failure (e.g. transient network outage, config error, code crash) has been resolved. The operator expects the job to run again with its full, configured retry budget. If `attempts` were preserved (remaining at `max_retries`), the job would have `0` retries remaining and would instantly transition back to `'dead'` on a single transient failure. Resetting to `0` gives the job a fresh retry budget.

---

### Q4: Graceful Shutdown Signaling
*   **Signaling Mechanism**: Database-backed worker registry.
*   **Considered & Rejected**:
    1.  **PID Files + OS Signals (`os.kill`)**: Rejected because sending SIGINT/SIGTERM is highly platform-dependent and fragile on Windows. If a worker crashes and orphans its PID file, and the OS subsequently assigns that PID to a system service, running `worker stop` could kill an unrelated program.
    2.  **Unix Domain Sockets**: Rejected because of poor cross-platform portability on Windows and increased code footprint.
*   **Why the database-backed registry was chosen**: Workers register themselves on start (storing their PIDs) and check the `stop_requested` flag inside their poll loops. It is 100% portable, immune to PID recycling issues, and integrates cleanly with worker heartbeat status checks.

---

### Q5: Priority Queues Retrospective
*   **What Survived Unchanged**:
    *   The core CAS update logic in `claim_next_job` (which is transaction-locked by SQLite).
    *   The worker execution loops, lease heartbeat renewer thread, and graceful shutdown state machine.
    *   The retry backoff scheduling logic.
*   **What Had to Change**:
    *   **Database Schema**: Migrated the `jobs` table to include a `priority` column.
    *   **Row Mapping**: The `Job` model row parser (`from_row`) had to extract the `priority` integer.
    *   **Job Selection Order**: The claim query's subquery sorting clause had to change from `ORDER BY created_at ASC` to `ORDER BY priority DESC, created_at ASC`.
    *   **CLI Enqueuing**: Updated `enqueue` to parse and insert the `priority` field.
