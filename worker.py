"""
Worker manager and worker logic.
"""
import threading
import subprocess
import time
import os
import shlex
from datetime import datetime
from config import Config
from storage import JobStorage

STOP_FLAG = os.path.join(os.path.dirname(__file__), "workers.stop")

class Worker(threading.Thread):
    def __init__(self, storage: JobStorage, wid:int):
        super().__init__(daemon=True)
        self.storage = storage
        self.stop_requested = False
        self.wid = wid
        self.cfg = Config()

    def run(self):
        poll = float(self.cfg.get("worker_poll_interval",1.0))
        while not self._should_stop():
            job = self.storage.fetch_and_lock_pending()
            if not job:
                time.sleep(poll)
                continue
            job_id = job.get("id")
            cmd = job.get("command")
            print(f"[worker-{self.wid}] picked job {job_id} -> {cmd}")
            start_ts = datetime.utcnow().isoformat() + "Z"
            try:
                # execute command using shell for compatibility with simple commands
                # use subprocess.run; capture output
                r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.cfg.get("job_timeout"))
                if r.returncode == 0:
                    self.storage.update_job_completion(job_id, "completed", attempts=job.get("attempts", 0))
                    print(f"[worker-{self.wid}] completed {job_id}")
                else:
                    err = r.stderr.decode('utf-8', errors='replace') or r.stdout.decode('utf-8', errors='replace')
                    print(f"[worker-{self.wid}] job {job_id} failed rc={r.returncode}: {err.strip()}")
                    attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                    # decide move to dead or schedule backoff by sleeping
                    if attempts >= maxr:
                        self.storage.move_to_dead(job_id, last_error=err[:1000])
                        print(f"[worker-{self.wid}] moved {job_id} to DLQ")
                    else:
                        backoff = (self.cfg.get("backoff_base",2) ** attempts)
                        print(f"[worker-{self.wid}] sleeping backoff {backoff}s for job {job_id} before next retry")
                        # Note: simple sleep for backoff. Another design is to store run_at; we keep simple behavior.
                        time.sleep(backoff)
            except subprocess.TimeoutExpired as toe:
                print(f"[worker-{self.wid}] job {job_id} timed out")
                attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                if attempts >= maxr:
                    self.storage.move_to_dead(job_id, last_error="timeout")
                    print(f"[worker-{self.wid}] moved {job_id} to DLQ (timeout)")
            except Exception as e:
                print(f"[worker-{self.wid}] exception running job {job_id}: {e}")
                attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                if attempts >= maxr:
                    self.storage.move_to_dead(job_id, last_error=str(e))

    def _should_stop(self):
        # stop if stop file present
        return os.path.exists(STOP_FLAG)

class WorkerManager:
    def __init__(self, db_path, count=1):
        self.storage = JobStorage(db_path)
        self.count = max(1, count)
        self.workers = []

    def run_forever(self):
        # spawn threads
        for i in range(self.count):
            w = Worker(self.storage, wid=i+1)
            w.start()
            self.workers.append(w)
        # join and wait for stop
        while True:
            time.sleep(0.5)
            if os.path.exists(STOP_FLAG):
                print("Stop flag detected; requesting workers to stop gracefully")
                break
            # also check threads alive
            alive = any(t.is_alive() for t in self.workers)
            if not alive:
                print("All workers finished")
                break
        # wait for threads to exit
        for t in self.workers:
            t.join(timeout=10)

    def stop(self):
        # create stop flag
        open(STOP_FLAG, "w").write("stop")

