# QueueCTL — CLI-Based Background Job Queue System

**QueueCTL** is a production-grade, Python-based CLI job queue system that manages background tasks with:
- Multiple workers  
- Retries with exponential backoff  
- Dead Letter Queue (DLQ)  
- Persistent SQLite storage  
- Configurable behavior  
- Web dashboard for monitoring  

Built fully in Python and designed to work perfectly on **Windows (PowerShell)** and **Linux**.

---

##  **Objective**

> Build a **CLI-based background job queue system** that can enqueue, process, retry, and monitor jobs efficiently, similar to Celery or Sidekiq, but lightweight and file-based.

---

## **Features**

| Category | Description |
|-----------|--------------|
|  **Job Management** | Enqueue, list, retry, and track jobs easily |
|  **Workers** | Parallel job processing with graceful shutdown |
|  **Retries** | Automatic retries with exponential backoff |
|  **Dead Letter Queue (DLQ)** | Stores permanently failed jobs for later inspection |
|  **Persistence** | SQLite-based job store, survives restarts |
|  **Configurable** | Adjustable retries, backoff base, timeout, etc. |
|  **Job Timeout Handling** | Cancels long-running jobs automatically |
|  **Priority Queues** | Processes high-priority jobs first |
|  **Scheduled Jobs** | Supports delayed execution via `run_at` |
|  **Job Logging** | Saves output (stdout/stderr) in `/logs` |
|  **Metrics Tracking** | Tracks job execution events and durations |
|  **Web Dashboard** | Live Flask-based dashboard for monitoring |
|  **Modular Architecture** | Separate files for storage, DLQ, config, worker, metrics, and web UI |

---

##  **Project Structure**
QueueCTL/

├─ queuectl.py # CLI entry point

├─ storage.py # SQLite job persistence

├─ worker.py # Worker management

├─ dlq.py # Dead Letter Queue logic

├─ config.py # Config manager

├─ metrics.py # Metrics collection

├─ web_dashboard.py # Flask web dashboard

├─ models.py # Data structures

├─ utils.py # Helper utilities

└─ logs/ # Job output logs


---

##  **Setup Instructions**

### Prerequisites
- Python 3.10 or higher
- Pip installed

###  Installation
```bash
git clone https://github.com/Bisu7/QueueCTL.git
cd QueueCTL
pip install -r requirements.txt
```
---


## Usage Examples

1️. Initialize Database

```python queuectl.py init-db```

2️. Enqueue a Job

```python queuectl.py enqueue '{"id":"job1","command":"echo Hello World"}'```

3. Start a Worker

```python queuectl.py worker start --count 2```

4️. Stop Workers

```python queuectl.py worker stop```

5️. Check Status

```python queuectl.py status```

6️. List Jobs by State

```python queuectl.py list --state pending```

7️. DLQ Operations
```
python queuectl.py dlq list
python queuectl.py dlq retry job1
```

8️. Change Configuration
```
python queuectl.py config set max_retries 5
python queuectl.py config set backoff_base 3
```

##  Bonus Features

 Job Timeout Handling

Automatically fails jobs exceeding job_timeout (configurable in queuectl_config.json).

 Priority Queues

Add ```"priority": <int>``` in enqueue JSON:

```python queuectl.py enqueue '{"id":"job_high","command":"echo High","priority":5}'```

 Scheduled / Delayed Jobs

Schedule job for later:

```python queuectl.py enqueue '{"id":"job_future","command":"echo Future Job","run_at":"10"}'```

 Job Output Logging

Check logs at:
```
logs/<job_id>.log
```

 Metrics

View collected metrics:
```
type metrics.json
```
---

##  Web Dashboard

Start dashboard:
```
python web_dashboard.py
```

Visit http://localhost:5000

You’ll see:

- Job counts by state
- Recent jobs table
- Auto-refresh
- Filter by state
- Color-coded statuses

##  **Architecture Overview**

###  Job Lifecycle
| **State** | **Description** |
|------------|----------------|
| `pending` | Waiting for worker |
| `processing` | Currently being executed |
| `completed` | Finished successfully |
| `failed` | Failed but retryable |
| `dead` | Permanently failed (moved to DLQ) |

###  Retry Logic
Exponential backoff is used to determine retry delay:
```text
delay = base ^ attempts
```

##  Worker Logic

- Fetch pending jobs (highest priority first)
- Execute via subprocess
- Handle timeout
- Retry failed jobs
- Move unrecoverable ones to DLQ
- Log output and update metrics

##  Testing Commands

| **Scenario** | **Command** |
|---------------|-------------|
| **Enqueue success** | `python queuectl.py enqueue '{"id":"ok","command":"echo OK"}'` |
| **Enqueue fail** | `python queuectl.py enqueue '{"id":"fail","command":"exit 1","max_retries":2}'` |
| **Run worker** | `python queuectl.py worker start` |
| **List DLQ** | `python queuectl.py dlq list` |
| **Retry DLQ job** | `python queuectl.py dlq retry fail` |
| **Start dashboard** | `python web_dashboard.py` |

## Configuration

All configurations are stored in `queuectl_config.json.`

Example:
```
{
  "max_retries": 3,
  "backoff_base": 2,
  "worker_poll_interval": 1.0,
  "job_timeout": 60
}
```
Change any parameter via CLI:
```
python queuectl.py config set job_timeout 30
```

## Sample Output
```
[worker-1] picked job job1 -> echo Hello World
[worker-1] completed job1
[worker-1] picked job jobfail -> exit 1
[worker-1] job jobfail failed rc=1:
[worker-1] sleeping backoff 2s for job jobfail before next retry
[worker-1] moved jobfail to DLQ
```
