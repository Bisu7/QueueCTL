import os
import sys
import sqlite3
from queuectl.db import get_connection, init_db, claim_next_job

DB_NAME = "priority_test.db"

def setup_db():
    if os.path.exists(DB_NAME):
        for ext in ["", "-wal", "-shm"]:
            try:
                os.remove(DB_NAME + ext)
            except OSError:
                pass
                
    conn = get_connection(DB_NAME)
    init_db(conn)
    
    # Enqueue in non-ordered sequence
    # 1. Low priority (1)
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, priority, created_at, updated_at)
        VALUES ('job-low', 'echo low', 'pending', 0, 3, 1, '2026-07-29T00:00:00Z', '2026-07-29T00:00:00Z');
        """
    )
    # 2. High priority (10)
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, priority, created_at, updated_at)
        VALUES ('job-high', 'echo high', 'pending', 0, 3, 10, '2026-07-29T00:00:01Z', '2026-07-29T00:00:01Z');
        """
    )
    # 3. Medium priority (5)
    conn.execute(
        """
        INSERT INTO jobs (id, command, state, attempts, max_retries, priority, created_at, updated_at)
        VALUES ('job-medium', 'echo medium', 'pending', 0, 3, 5, '2026-07-29T00:00:02Z', '2026-07-29T00:00:02Z');
        """
    )
    conn.commit()
    conn.close()

def run_test():
    setup_db()
    print("Database configured.")
    
    conn = get_connection(DB_NAME)
    
    # Claim first job
    j1 = claim_next_job(conn, "worker-test")
    print("First claimed:", j1.id if j1 else None)
    assert j1 is not None and j1.id == "job-high"
    
    # Claim second job
    j2 = claim_next_job(conn, "worker-test")
    print("Second claimed:", j2.id if j2 else None)
    assert j2 is not None and j2.id == "job-medium"
    
    # Claim third job
    j3 = claim_next_job(conn, "worker-test")
    print("Third claimed:", j3.id if j3 else None)
    assert j3 is not None and j3.id == "job-low"
    
    conn.close()
    
    # Cleanup DB files
    for ext in ["", "-wal", "-shm"]:
        try:
            os.remove(DB_NAME + ext)
        except OSError:
            pass
            
    print("SUCCESS: Priority execution ordering verified successfully!")

if __name__ == "__main__":
    run_test()
