import os
import sys
import subprocess
import time
import sqlite3
from queuectl.db import get_connection, init_db

DB_NAME = "dlq_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    conn.close()

def log(msg):
    print(f"[TEST] {msg}")

def run_command(args):
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli"] + args,
        env=env,
        capture_output=True,
        text=True
    )
    if res.returncode != 0:
        print(f"Command failed: {args}")
        print(f"Stdout: {res.stdout}")
        print(f"Stderr: {res.stderr}")
    return res

def run_test():
    setup_db()
    log("Database setup completed.")
    
    # 1. Test config set
    log("Setting max-retries to 2...")
    res = run_command(["config", "set", "max-retries", "2"])
    assert res.returncode == 0
    
    log("Setting backoff-base to 3...")
    res = run_command(["config", "set", "backoff-base", "3"])
    assert res.returncode == 0
    
    # Test config get
    res = run_command(["config", "get", "max-retries"])
    assert res.stdout.strip() == "2"
    log(f"Config get max-retries: {res.stdout.strip()} (expected 2)")
    
    res = run_command(["config", "get", "backoff-base"])
    assert res.stdout.strip() == "3"
    log(f"Config get backoff-base: {res.stdout.strip()} (expected 3)")
    
    # 2. Enqueue a failing job
    log("Enqueuing failing job...")
    res = run_command(["enqueue", '{"id": "fail-job", "command": "exit 1"}'])
    assert res.returncode == 0
    
    # Verify enqueued max_retries is 2
    conn = get_connection(DB_NAME)
    r = conn.execute("SELECT max_retries FROM jobs WHERE id = 'fail-job';").fetchone()
    assert r["max_retries"] == 2
    log(f"Enqueued job max_retries: {r['max_retries']} (expected 2)")
    conn.close()
    
    # 3. Start worker to execute and fail the job until it transitions to dead
    log("Starting worker in background...")
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    worker = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    # Wait for:
    # - Run 1: attempts = 1, backoff = 3^1 = 3 seconds.
    # - Run 2: attempts = 2. Since 2 >= max_retries (2), state becomes 'dead'.
    # Sleep 8 seconds to allow this timeline to complete.
    log("Waiting for worker to run, backoff (3s), and fail job...")
    time.sleep(8.0)
    
    # Stop worker gracefully
    log("Stopping worker...")
    run_command(["worker", "stop"])
    worker.wait()
    
    # 4. Verify job is in DLQ
    res = run_command(["dlq", "list"])
    log(f"DLQ list output:\n{res.stdout.strip()}")
    assert "fail-job" in res.stdout
    
    # Verify job is dead in DB
    conn = get_connection(DB_NAME)
    r = conn.execute("SELECT state, attempts FROM jobs WHERE id = 'fail-job';").fetchone()
    assert r["state"] == "dead"
    assert r["attempts"] == 2
    log(f"Job state in DB: {r['state']} | Attempts: {r['attempts']} (expected dead | 2)")
    conn.close()
    
    # 5. Test DLQ retry
    log("Retrying job from DLQ...")
    res = run_command(["dlq", "retry", "fail-job"])
    assert res.returncode == 0
    log(res.stdout.strip())
    
    # Verify state and attempts reset
    conn = get_connection(DB_NAME)
    r = conn.execute("SELECT state, attempts, max_retries FROM jobs WHERE id = 'fail-job';").fetchone()
    assert r["state"] == "pending"
    assert r["attempts"] == 0
    assert r["max_retries"] == 2
    log(f"Job state after retry: {r['state']} | Attempts: {r['attempts']} (expected pending | 0)")
    conn.close()
    
    # 6. Change global config max-retries to 10
    log("Changing global max-retries config to 10...")
    res = run_command(["config", "set", "max-retries", "10"])
    assert res.returncode == 0
    
    # Verify that the already enqueued job's max_retries remains 2 (frozen)
    conn = get_connection(DB_NAME)
    r = conn.execute("SELECT max_retries FROM jobs WHERE id = 'fail-job';").fetchone()
    assert r["max_retries"] == 2
    log(f"Frozen job max_retries: {r['max_retries']} (expected 2 despite global config change to 10)")
    conn.close()
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    log("SUCCESS: DLQ and config test completed successfully!")

if __name__ == "__main__":
    run_test()
