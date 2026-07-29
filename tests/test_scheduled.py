import os
import sys
import sqlite3
import datetime
import time
from queuectl.db import get_connection, init_db, claim_next_job

DB_NAME = "scheduled_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    now = datetime.datetime.now(datetime.timezone.utc)
    future = (now + datetime.timedelta(seconds=4)).isoformat()
    past = (now - datetime.timedelta(seconds=10)).isoformat()
    
    # 1. Future job
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, priority, run_at, created_at, updated_at)
        VALUES ('job-future', 'echo future', 'pending', 0, 3, 0, ?, ?, ?);
        """,
        (future, now.isoformat(), now.isoformat())
    )
    # 2. Past job
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, priority, run_at, created_at, updated_at)
        VALUES ('job-past', 'echo past', 'pending', 0, 3, 0, ?, ?, ?);
        """,
        (past, now.isoformat(), now.isoformat())
    )
    conn.commit()
    conn.close()

def run_test():
    setup_db()
    print("Database configured.")
    
    conn = get_connection(DB_NAME)
    
    # Claim first job immediately (should be job-past)
    j1 = claim_next_job(conn, "worker-test")
    print("First claimed:", j1.id if j1 else None)
    assert j1 is not None and j1.id == "job-past"
    
    # Try to claim again (should be None since job-future is in the future)
    j2 = claim_next_job(conn, "worker-test")
    print("Second claimed (before wait):", j2.id if j2 else None)
    assert j2 is None
    
    # Wait for the future job to become eligible (4 seconds sleep)
    print("Waiting 5.0 seconds for job-future to mature...")
    time.sleep(5.0)
    
    # Try to claim again (should be job-future now)
    j3 = claim_next_job(conn, "worker-test")
    print("Third claimed (after wait):", j3.id if j3 else None)
    assert j3 is not None and j3.id == "job-future"
    
    conn.close()
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    print("SUCCESS: Scheduled job execution checked successfully!")

if __name__ == "__main__":
    run_test()
