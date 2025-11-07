import time, signal
import json, os
from db import getConn
from datetime import datetime, timedelta
import multiprocessing
from executor import run_command

shutdownFlag = multiprocessing.Event()

def signalHandle(signum, frame):
    print(f"[Worker] Received signal {signum}, shutting down gracefully...")
    shutdownFlag.set()


signal.signal(signal.SIGINT, signalHandle)
signal.signal(signal.SIGTERM, signalHandle)

#single next job claim function
def getNextJob(worker_ID: str):
    now = datetime.utcnow().isoformat()
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute('BEGIN')

        cur.execute("""
            SELECT id FROM jobs
            WHERE state = 'pending' AND nextRunAt <= ?
            ORDER BY createdAt
            LIMIT 1
        """,(now,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        
        jobId = row[0]
        cur.execute("""
            UPDATE jobs
            SET state='processing', lockedBy = ?, attempts=attempts+1, updatedAt=?
            WHERE id=? AND state='pending'
        """,(worker_ID, now, jobId))

        if cur.rowcount == 1:
            conn.commit()
            cur.execute("SELECT * FROM jobs WHERE id=?",(jobId,))
            return cur.fetchone()
        else:
            conn.rollback()
            return None

#single job processing 
def jobProcess(job_row, worker_id):
    (
        job_id,
        command,
        state,
        attempts,
        maxRetries,
        baseBackoff,
        createdAt,
        updatedAt,
        nextRunAt,
        lockedBy,
        timeout,
        outputLog
    ) = job_row

    print(f"[{worker_id}] is Processing job {job_id}: '{command}' (Attempt {attempts})")

    result = run_command(command, timeout)
    exit_code = result["exit_code"]
    now = datetime.utcnow().isoformat()

    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN")

        if exit_code == 0:
            print(f"[{worker_id}] is completed with Job {job_id} successfully.")
            cur.execute("""
                UPDATE jobs
                SET state='completed', updated_at=?, output_log=?
                WHERE id=?
            """, (now, json.dumps(result), job_id))

        else:
            if attempts >= maxRetries:
                print(f"[{worker_id}] has Job {job_id} moved to Dead Letter Queue.")
                cur.execute("""
                    UPDATE jobs
                    SET state='dead', updated_at=?, output_log=?
                    WHERE id=?
                """, (now, json.dumps(result), job_id))
            else:
                delay = baseBackoff ** attempts
                nextRunTime = (datetime.utcnow() + timedelta(seconds=delay)).isoformat()
                print(f"[{worker_id}] is Retrying job {job_id} after {delay}s (Attempt {attempts}/{maxRetries})")
                cur.execute("""
                    UPDATE jobs
                    SET state='pending', next_run_at=?, updated_at=?, locked_by=NULL, output_log=?
                    WHERE id=?
                """, (nextRunTime, now, json.dumps(result), job_id))

        conn.commit()


def worker_loop(worker_id: str, poll_interval: int = 1):
    print(f"[{worker_id}] Worker started (PID={os.getpid()})")

    while not shutdownFlag.is_set():
        job = getNextJob(worker_id)
        if job:
            jobProcess(job, worker_id)
        else:
            time.sleep(poll_interval)

    print(f"[{worker_id}] shutdown complete.")


# Start Multiple Workers
def start_workers(count: int = 1, poll_interval: int = 1):
    processes = []

    for i in range(count):
        worker_id = f"worker-{i+1}"
        p = multiprocessing.Process(
            target=worker_loop,
            args=(worker_id, poll_interval),
            daemon=False
        )
        p.start()
        processes.append(p)
        print(f"[Main] Started {worker_id} (PID={p.pid})")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("[Main] Received keyboard interrupt, stopping workers.")
        shutdownFlag.set()
        for p in processes:
            p.join()

    print("[Main] All workers stopped.")