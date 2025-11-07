import json
from db import getConn
from utils import now_iso

def enqueueJobs(job_json: str):
    job = json.loads(job_json)
    job.setdefault('state', 'pending')
    job.setdefault('attempts',0)
    job.setdefault('maxRetries',3)
    job.setdefault('baseBackoff',2)
    job.setdefault('timeout',None)
    created = now_iso()
    job.setdefault('createdAt',created)
    job.setdefault('updatedAt',created)
    job.setdefault('nextRunAt',created)

    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs
                (id,command, state, attempts, maxRetries, baseBackoff, createdAt,updatedAt, nextRunAt,timeout,outputLog)
                VALUES (?,?,? ,?,?, ?, ?, ?,?,?,?)
        """,(
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