import json, os, time
METRICS_PATH = os.path.join(os.path.dirname(__file__), "metrics.json")

def record(event_type, job_id=None, status=None):
    data = {}
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH, "r") as f:
            try:
                data = json.load(f)
            except:
                data = {}
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    data.setdefault("events", []).append({
        "timestamp": now,
        "event": event_type,
        "job_id": job_id,
        "status": status
    })
    with open(METRICS_PATH, "w") as f:
        json.dump(data, f, indent=2)
