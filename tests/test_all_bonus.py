import os
import sys
import subprocess
import time
from queuectl.db import get_connection, init_db

DB_NAME = "bonus_test.db"

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
    
    # Set backoff-base to 20 to prevent timeout-job from retrying during the test
    log("Setting backoff-base config to 20...")
    res = run_command(["config", "set", "backoff-base", "20"])
    assert res.returncode == 0
    
    # 1. Enqueue job with timeout_seconds = 2, sleeping 5 seconds
    log("Enqueuing job that will timeout (sleeps 5s, timeout 2s)...")
    res = run_command([
        "enqueue", 
        '{"id": "timeout-job", "command": "python -c \\"import time; time.sleep(5)\\"", "timeout_seconds": 2}'
    ])
    assert res.returncode == 0
    
    # Enqueue a successful job to capture its normal output
    log("Enqueuing successful job...")
    res = run_command([
        "enqueue", 
        '{"id": "success-job", "command": "echo hello"}'
    ])
    assert res.returncode == 0
    
    # 2. Start worker to execute
    log("Starting background worker...")
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    worker = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    log("Waiting for worker to run jobs...")
    # Wait 8 seconds to allow worker start lag + timeout-job timeout + success-job run
    time.sleep(8.0)
    
    log("Stopping worker...")
    run_command(["worker", "stop"])
    worker.wait()
    
    # 3. Check logs of success-job
    log("Checking logs of successful job...")
    res = run_command(["logs", "success-job"])
    assert res.returncode == 0
    print("--- success-job logs ---")
    print(res.stdout.strip())
    print("------------------------")
    assert "hello" in res.stdout
    
    # 4. Check logs of timeout-job
    log("Checking logs of timed out job...")
    res = run_command(["logs", "timeout-job"])
    assert res.returncode == 0
    print("--- timeout-job logs ---")
    print(res.stdout.strip())
    print("------------------------")
    assert "TIMEOUT EXPIRED" in res.stdout
    
    # Verify timeout-job is in failed state in DB
    conn = get_connection(DB_NAME)
    r = conn.execute("SELECT state, attempts FROM jobs WHERE id = 'timeout-job';").fetchone()
    assert r["state"] == "failed"
    assert r["attempts"] == 1
    log(f"timeout-job state: {r['state']} | attempts: {r['attempts']} (expected failed | 1)")
    conn.close()
    
    # 5. Check metrics CLI
    log("Testing metrics command...")
    res = run_command(["metrics", "--minutes", "10"])
    assert res.returncode == 0
    print("--- Metrics Output ---")
    print(res.stdout.strip())
    print("----------------------")
    assert "Completed Jobs" in res.stdout
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    log("SUCCESS: All remaining bonus features verified successfully!")

if __name__ == "__main__":
    run_test()
