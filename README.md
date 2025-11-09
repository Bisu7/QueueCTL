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
