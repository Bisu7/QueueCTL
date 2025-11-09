# 🚀 QueueCTL — CLI-Based Background Job Queue System

**QueueCTL** is a production-grade, Python-based CLI job queue system that manages background tasks with:
- Multiple workers  
- Retries with exponential backoff  
- Dead Letter Queue (DLQ)  
- Persistent SQLite storage  
- Configurable behavior  
- Web dashboard for monitoring  

Built fully in Python and designed to work perfectly on **Windows (PowerShell)** and **Linux**.

---

## 🎯 **Objective**

> Build a **CLI-based background job queue system** that can enqueue, process, retry, and monitor jobs efficiently, similar to Celery or Sidekiq, but lightweight and file-based.

---

## 🧩 **Features**

| Category | Description |
|-----------|--------------|
| 🧾 **Job Management** | Enqueue, list, retry, and track jobs easily |
| ⚙️ **Workers** | Parallel job processing with graceful shutdown |
| 🔁 **Retries** | Automatic retries with exponential backoff |
| ☠️ **Dead Letter Queue (DLQ)** | Stores permanently failed jobs for later inspection |
| 💾 **Persistence** | SQLite-based job store, survives restarts |
| 🧮 **Configurable** | Adjustable retries, backoff base, timeout, etc. |
| ⏱️ **Job Timeout Handling** | Cancels long-running jobs automatically |
| 🎚️ **Priority Queues** | Processes high-priority jobs first |
| ⏰ **Scheduled Jobs** | Supports delayed execution via `run_at` |
| 📄 **Job Logging** | Saves output (stdout/stderr) in `/logs` |
| 📊 **Metrics Tracking** | Tracks job execution events and durations |
| 🌐 **Web Dashboard** | Live Flask-based dashboard for monitoring |
| 🧱 **Modular Architecture** | Separate files for storage, DLQ, config, worker, metrics, and web UI |

---

## 📁 **Project Structure**
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

## ⚙️ **Setup Instructions**

### 🧰 Prerequisites
- Python 3.10 or higher
- Pip installed

### 🧩 Installation
```bash
git clone https://github.com/Bisu7/QueueCTL.git
cd QueueCTL
pip install -r requirements.txt
```


---



##💻 Usage Examples
1️⃣ Initialize Database
python queuectl.py init-db

2️⃣ Enqueue a Job
python queuectl.py enqueue '{"id":"job1","command":"echo Hello World"}'

3️⃣ Start a Worker
python queuectl.py worker start --count 2

4️⃣ Stop Workers
python queuectl.py worker stop

5️⃣ Check Status
python queuectl.py status

6️⃣ List Jobs by State
python queuectl.py list --state pending

7️⃣ DLQ Operations
python queuectl.py dlq list
python queuectl.py dlq retry job1

8️⃣ Change Configuration
python queuectl.py config set max_retries 5
python queuectl.py config set backoff_base 3

🌈 Bonus Features
🕓 Job Timeout Handling

Automatically fails jobs exceeding job_timeout (configurable in queuectl_config.json).

🚦 Priority Queues

Add "priority": <int> in enqueue JSON:

python queuectl.py enqueue '{"id":"job_high","command":"echo High","priority":5}'

⏰ Scheduled / Delayed Jobs

Schedule job for later:

python queuectl.py enqueue '{"id":"job_future","command":"echo Future Job","run_at":"10"}'

🧾 Job Output Logging

Check logs at:

logs/<job_id>.log

📊 Metrics

View collected metrics:

type metrics.json
