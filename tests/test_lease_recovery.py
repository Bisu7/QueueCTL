import os
import sys
import subprocess
import time
import datetime
import signal
from queuectl.db import get_connection, init_db

DB_NAME = "lease_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    # Enqueue a slow job (sleeps for 10 seconds)
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES ('slow-job', 'python -c "import time; time.sleep(10)"', 'pending', 0, 3, ?, ?);
        """,
        (datetime.datetime.now(datetime.timezone.utc).isoformat(), datetime.datetime.now(datetime.timezone.utc).isoformat())
    )
    
    # Set lease_duration to 5 seconds for fast testing
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('lease_duration', '5');")
    # Set retry_backoff_base to 2 seconds
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('retry_backoff_base', '2');")
    conn.commit()
    conn.close()

def log(msg):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {msg}")

def run_test():
    setup_db()
    log("Database configured. lease_duration = 5 seconds.")
    
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    
    # 1. Spawn Worker 1
    log("Spawning Worker 1...")
    w1 = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    # Wait for Worker 1 to claim the job
    time.sleep(2.0)
    
    # Inspect DB to confirm Worker 1 claimed it
    conn = get_connection(DB_NAME)
    job = conn.execute("SELECT state, attempts, locked_by, lease_expires_at FROM jobs WHERE id = 'slow-job';").fetchone()
    log(f"Current Job State: {job['state']} | Attempts: {job['attempts']} | Locked by: {job['locked_by']} | Expires: {job['lease_expires_at']}")
    conn.close()
    
    # 2. SIGKILL Worker 1 (simulating sudden crash)
    log("SIGKILLing Worker 1 mid-job...")
    w1.kill()
    w1.wait()
    
    # Wait for the lease to expire (total 5s from claim. We already slept 2s. Let's sleep 4s to guarantee expiry).
    log("Waiting 4.0 seconds for lease expiration...")
    time.sleep(4.0)
    
    # 3. Spawn Worker 2
    log("Spawning Worker 2...")
    w2 = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    # Wait for Worker 2 to reap, wait out the 2-second backoff delay, reclaim the job, and run it to completion (10s sleep)
    # Total wait time: 15 seconds
    log("Waiting 15.0 seconds for Worker 2 to reap, backoff (2s), reclaim, and finish (10s)...")
    time.sleep(15.0)
    
    # Stop Worker 2 gracefully
    log("Stopping Worker 2 gracefully...")
    try:
        w2.send_signal(signal.SIGINT)
    except Exception:
        w2.terminate()
    w2.wait()
    
    # Check final state in DB
    conn = get_connection(DB_NAME)
    job = conn.execute("SELECT state, attempts, locked_by, next_attempt_at FROM jobs WHERE id = 'slow-job';").fetchone()
    log(f"Final Job State: {job['state']} | Attempts: {job['attempts']} | Locked by: {job['locked_by']}")
    conn.close()
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    if job['state'] != 'completed':
        log(f"FAIL: Job state is {job['state']}, expected completed")
        sys.exit(1)
        
    if job['attempts'] != 2:
        log(f"FAIL: Job has {job['attempts']} attempts, expected 2 (1 crashed, 1 completed)")
        sys.exit(1)
        
    log("SUCCESS: Lease recovery test completed successfully!")

if __name__ == "__main__":
    run_test()
