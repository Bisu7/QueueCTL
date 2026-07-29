import os
import sys
import subprocess
import time
import datetime
from queuectl.db import get_connection, init_db

DB_NAME = "stop_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    # Enqueue a job that sleeps for 4 seconds
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES ('slow-job', 'python -c "import time; time.sleep(4)"', 'pending', 0, 3, ?, ?);
        """,
        (datetime.datetime.now(datetime.timezone.utc).isoformat(), datetime.datetime.now(datetime.timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

def log(msg):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {msg}")

def run_test():
    setup_db()
    log("Database setup completed.")
    
    env = os.environ.copy()
    env["QUEUECTL_DB"] = DB_NAME
    
    # Spawn worker
    log("Spawning worker...")
    worker = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli", "worker", "start", "--count", "1"],
        env=env
    )
    
    # Wait for worker to claim the job and register
    time.sleep(1.5)
    
    # Run status command and capture output
    log("Checking status...")
    status_out = subprocess.run(
        [sys.executable, "-m", "queuectl.cli", "status"],
        env=env,
        capture_output=True,
        text=True
    ).stdout
    print("--- Status Command Output ---")
    print(status_out.strip())
    print("-----------------------------")
    
    # Check that worker is registered in DB
    conn = get_connection(DB_NAME)
    from queuectl.db import count_active_workers
    active_before = count_active_workers(conn)
    log(f"Active workers count in DB: {active_before}")
    conn.close()
    
    if active_before != 1:
        log("FAIL: Worker did not register as active.")
        worker.terminate()
        sys.exit(1)
        
    # Trigger stop command in a separate process
    log("Running worker stop command...")
    stop_start = time.time()
    stop_proc = subprocess.run(
        [sys.executable, "-m", "queuectl.cli", "worker", "stop"],
        env=env,
        capture_output=True,
        text=True
    )
    stop_duration = time.time() - stop_start
    log(f"Stop command completed in {stop_duration:.2f} seconds.")
    print("--- Stop Command Output ---")
    print(stop_proc.stdout.strip())
    print("---------------------------")
    
    worker.wait()
    log("Worker process has exited.")
    
    # Verify final states
    conn = get_connection(DB_NAME)
    job_state = conn.execute("SELECT state FROM jobs WHERE id = 'slow-job';").fetchone()["state"]
    active_after = count_active_workers(conn)
    log(f"Final Job State: {job_state} (expected completed)")
    log(f"Active workers in DB: {active_after} (expected 0)")
    conn.close()
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    if job_state != 'completed':
        log("FAIL: Job did not complete before worker stopped.")
        sys.exit(1)
        
    if active_after != 0:
        log("FAIL: Worker failed to unregister.")
        sys.exit(1)
        
    log("SUCCESS: Graceful stop and status check verified successfully!")

if __name__ == "__main__":
    run_test()
