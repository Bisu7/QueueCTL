import os
import sys
import subprocess
import time
import datetime
import signal
from queuectl.db import get_connection, init_db

DB_NAME = "worker_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    # Enqueue a success job
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES ('job-success', 'echo ok', 'pending', 0, 3, ?, ?);
        """,
        (datetime.datetime.now(datetime.timezone.utc).isoformat(), datetime.datetime.now(datetime.timezone.utc).isoformat())
    )
    
    # Enqueue a failing job (max_retries = 2)
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES ('job-fail', 'exit 1', 'pending', 0, 2, ?, ?);
        """,
        (datetime.datetime.now(datetime.timezone.utc).isoformat(), datetime.datetime.now(datetime.timezone.utc).isoformat())
    )
    
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('retry_backoff_base', '2');")
    conn.commit()
    conn.close()

def run_test():
    setup_db()
    print("Database set up with test jobs.")
    
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    p = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    print("Worker started. Waiting for jobs to execute and retry...")
    
    time.sleep(6)
    
    print("Sending SIGINT to worker process...")
    try:
        p.send_signal(signal.SIGINT)
    except Exception:
        p.terminate()
        
    p.wait()
    print("Worker stopped.")
    
    conn = get_connection(DB_NAME)
    rows = conn.execute("SELECT id, state, attempts, next_attempt_at FROM jobs ORDER BY id;").fetchall()
    
    print("\n--- Job States in DB ---")
    for r in rows:
        print(f"ID: {r['id']:<12} | State: {r['state']:<10} | Attempts: {r['attempts']} | Next attempt: {r['next_attempt_at']}")
        
    conn.close()
    
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass

    states = {r['id']: r['state'] for r in rows}
    attempts = {r['id']: r['attempts'] for r in rows}
    
    if states.get('job-success') != 'completed':
        print(f"FAIL: job-success is in state {states.get('job-success')}, expected completed")
        sys.exit(1)
        
    if states.get('job-fail') != 'dead':
        print(f"FAIL: job-fail is in state {states.get('job-fail')}, expected dead")
        sys.exit(1)
        
    if attempts.get('job-fail') != 2:
        print(f"FAIL: job-fail has {attempts.get('job-fail')} attempts, expected 2")
        sys.exit(1)
        
    print("\nSUCCESS: Worker execution, backoff retries, and DLQ dead states are correct!")

if __name__ == "__main__":
    run_test()
