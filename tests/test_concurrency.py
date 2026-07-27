import os
import sys
import subprocess
import sqlite3
import time
from queuectl.db import get_connection, init_db

DB_NAME = "concurrency_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        # Clean up any files from a previous run
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    # Enqueue 50 test jobs
    for i in range(1, 51):
        job_id = f"job-{i}"
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (job_id, f"echo task {i}", "pending", 0, 3, "2026-07-27T00:00:00Z", "2026-07-27T00:00:00Z")
        )
    conn.commit()
    conn.close()

# The code that each worker process will execute
WORKER_CODE = """
import sys
import time
from queuectl.db import get_connection, claim_next_job

db_path = sys.argv[1]
worker_id = sys.argv[2]
claimed = []

# Open its own connection
conn = get_connection(db_path)
while True:
    job = claim_next_job(conn, worker_id)
    if job is None:
        break
    claimed.append(job.id)
    time.sleep(0.005) # small sleep to introduce concurrency overlap
conn.close()

# Print the claimed jobs space-separated
print(" ".join(claimed))
"""

def run_test():
    setup_db()
    print("Database set up with 50 pending jobs.")
    
    processes = []
    for i in range(4):
        worker_id = f"worker-{i}"
        p = subprocess.Popen(
            [sys.executable, "-c", WORKER_CODE, DB_NAME, worker_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        processes.append((worker_id, p))
    
    print("4 worker processes spawned concurrently. Waiting for completion...")
    
    all_claimed = {}
    for worker_id, p in processes:
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            print(f"Error in {worker_id}: {stderr}", file=sys.stderr)
            sys.exit(1)
        
        jobs = stdout.strip().split()
        all_claimed[worker_id] = jobs
        print(f"{worker_id} claimed {len(jobs)} jobs: {jobs[:5]} ...")

    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass

    total_claims = 0
    seen_jobs = set()
    duplicates = set()
    
    for worker_id, jobs in all_claimed.items():
        for job in jobs:
            total_claims += 1
            if job in seen_jobs:
                duplicates.add(job)
            seen_jobs.add(job)
            
    print("\n--- Concurrency Verification Summary ---")
    print(f"Total jobs enqueued: 50")
    print(f"Total claims made:   {total_claims}")
    print(f"Unique jobs claimed: {len(seen_jobs)}")
    print(f"Duplicate claims:    {len(duplicates)}")
    
    if duplicates:
        print(f"FAIL: The following jobs were claimed by multiple workers: {duplicates}")
        sys.exit(1)
    elif len(seen_jobs) != 50:
        print(f"FAIL: Not all jobs were claimed (only claimed {len(seen_jobs)} out of 50).")
        sys.exit(1)
    else:
        print("SUCCESS: Every job was claimed exactly once. Atomic locking verified.")

if __name__ == "__main__":
    run_test()
