import time, signal
import json
from db import getConn
from datetime import datetime

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

shutdown = False

def signalHandling(sig,frame):
    global shutdown
    shutdown = True