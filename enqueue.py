from db import getConn
from utils import now_iso

def enqueueJobs(job_str: str):
    # Split the input string: first token = job id, rest = command
    parts = job_str.strip().split(" ", 1)
    if len(parts) < 2:
        print("❌ Error: Provide both job ID and command string.")
        return

    job_id, command = parts[0], parts[1]

    # Default job structure
    job = {
        'id': job_id,
        'command': command,
        'state': 'pending',
        'attempts': 0,
        'maxRetries': 3,
        'baseBackoff': 2,
        'timeout': None,
        'outputLog': None
    }

    created = now_iso()
    job['createdAt'] = created
    job['updatedAt'] = created
    job['nextRunAt'] = created

    # Insert job into SQLite database
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs
                (id, command, state, attempts, maxRetries, baseBackoff, createdAt, updatedAt, nextRunAt, timeout, outputLog)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job['id'],
            job['command'],
            job['state'],
            job['attempts'],
            job['maxRetries'],
            job['baseBackoff'],
            job['createdAt'],
            job['updatedAt'],
            job['nextRunAt'],
            job['timeout'],
            job['outputLog'],
        ))
        conn.commit()

    print(f"✅ Job '{job_id}' enqueued successfully with command: {command}")
