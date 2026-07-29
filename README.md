# QueueCTL: Background Job Queue CLI

`queuectl` is a lightweight, high-performance background job queue CLI in Python. It uses SQLite for metadata persistence, enforces concurrency guarantees with atomic transactions, supports leases for crash recovery, handles priority/scheduled jobs, and features a minimal web dashboard.

---

## Architecture Overview

```text
               +-----------------------+
               |  queuectl enqueue     |
               +-----------+-----------+
                           |
                           v
               +-----------+-----------+
               |  SQLite Database      | <-----+  queuectl config
               |  (WAL Mode enabled)   |       |  queuectl status
               +-----------+-----------+       |  queuectl metrics
                           |                   |  queuectl dashboard
                           v (Atomic Claim)    |
               +-----------+-----------+       |
               |  Worker Process (N)   | ------+
               |  - Runs Command       |
               |  - Lease Renewer      |
               +-----+-----------+-----+
                     |           |
                     | (Success) | (Failure / Timeout)
                     v           v
            +-----------+     +-----------+
            | Completed |     |  Failed   | (Attempts < Max Retries)
            +-----------+     +-----+-----+
                                    |
                                    v (Attempts >= Max Retries)
                              +-----------+
                              | Dead(DLQ) |
                              +-----------+
```

---

## Installation & Setup

### 1. Requirements
*   Python 3.8 or higher.
*   Standard library packages only (sqlite3, multiprocessing, etc.). Click is the only external dependency.

### 2. Install
Clone this repository, navigate to the folder, and install it locally in editable mode:
```bash
pip install -e .
```

This registers the `queuectl` command group globally on your system.

---

## CLI Usage Guide

### 1. General Commands
*   **Enqueue a job**:
    ```bash
    queuectl enqueue '{"id": "job1", "command": "echo hello"}'
    ```
    To set optional fields (priority, run_at, timeout_seconds):
    ```bash
    queuectl enqueue '{"id": "job2", "command": "ping 127.0.0.1 -n 5", "priority": 10, "timeout_seconds": 3}'
    ```
*   **List jobs**:
    ```bash
    queuectl list
    # Or output strictly as raw JSON:
    queuectl list --json
    ```
*   **Show queue status**:
    ```bash
    queuectl status
    ```

### 2. Worker Lifecycle
*   **Start workers**:
    ```bash
    queuectl worker start --count 2
    ```
*   **Stop workers gracefully**:
    ```bash
    queuectl worker stop
    ```

### 3. Configuration Management
*   **Set config values**:
    ```bash
    queuectl config set max-retries 5
    queuectl config set backoff-base 3
    ```
*   **Get config values**:
    ```bash
    queuectl config get max-retries
    ```

### 4. DLQ Management
*   **List dead jobs**:
    ```bash
    queuectl dlq list
    ```
*   **Retry a dead job**:
    ```bash
    queuectl dlq retry job2
    ```

### 5. Output Logs & Metrics
*   **View execution logs**:
    ```bash
    queuectl logs job2
    ```
*   **Show throughput metrics**:
    ```bash
    queuectl metrics --minutes 15
    ```

### 6. Web Dashboard
*   **Start the dashboard**:
    ```bash
    queuectl dashboard --port 8000
    ```
    Then open `http://localhost:8000/` in your browser.

---

## Running Verification Tests

Run the automated test scripts to verify worker stop behaviors, config persistence, scheduling, and error handling:
```bash
# Verify graceful cross-terminal worker shutdown
python tests/test_worker_stop.py

# Verify DLQ retries and frozen job retry budgets
python tests/test_dlq_config.py

# Verify priority scheduling
python tests/test_priority.py

# Verify scheduled jobs (run_at)
python tests/test_scheduled.py

# Verify timeouts, execution logging, and metrics
python tests/test_all_bonus.py
```

---

## Demo Recording
*   [\[Demo video link placeholder\]](https://www.loom.com/share/f9d101b787ec44acbf1641117d055b8c)
