import threading
import subprocess
import time
import os
from datetime import datetime, timezone
from config import Config
from storage import JobStorage
from metrics import record  # ✅ Metrics import

STOP_FLAG = os.path.join(os.path.dirname(__file__), "workers.stop")

class Worker(threading.Thread):
    def __init__(self, storage: JobStorage, wid:int):
        super().__init__(daemon=True)
        self.storage = storage
        self.stop_requested = False
        self.wid = wid
        self.cfg = Config()

    def _log_output(self, job_id, stdout, stderr):
        import os
        from datetime import datetime
        os.makedirs("logs", exist_ok=True)
        with open(f"logs/{job_id}.log", "a", encoding="utf-8") as f:
            f.write(f"\n==== {datetime.now(timezone.utc).isoformat()} ====\n")
            if stdout:
                f.write(f"STDOUT:\n{stdout}\n")
            if stderr:
                f.write(f"STDERR:\n{stderr}\n")

    def run(self):
        poll = float(self.cfg.get("worker_poll_interval", 1.0))
        while not self._should_stop():
            job = self.storage.fetch_and_lock_pending()
            if not job:
                time.sleep(poll)
                continue

            job_id = job.get("id")
            cmd = job.get("command")
            print(f"[worker-{self.wid}] picked job {job_id} -> {cmd}")

            # ✅ Record job start event
            record("start", job_id=job_id)

            start_ts = datetime.now(timezone.utc).isoformat()
            try:
                timeout = self.cfg.get("job_timeout")
                try:
                    r = subprocess.run(
                        cmd,
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=timeout
                    )

                    # decode and log job output
                    out = r.stdout.decode('utf-8', errors='replace')
                    err = r.stderr.decode('utf-8', errors='replace')
                    self._log_output(job_id, out, err)

                except subprocess.TimeoutExpired:
                    print(f"[worker-{self.wid}] job {job_id} timed out after {timeout}s")
                    attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                    if attempts >= maxr:
                        self.storage.move_to_dead(job_id, last_error="timeout")
                        print(f"[worker-{self.wid}] moved {job_id} to DLQ (timeout)")

                        # ✅ Record DLQ move due to timeout
                        record("complete", job_id=job_id, status="timeout")

                    continue

                if r.returncode == 0:
                    self.storage.update_job_completion(job_id, "completed", attempts=job.get("attempts", 0))
                    print(f"[worker-{self.wid}] completed {job_id}")

                    # ✅ Record successful completion
                    record("complete", job_id=job_id, status="success")

                else:
                    err = r.stderr.decode('utf-8', errors='replace') or r.stdout.decode('utf-8', errors='replace')
                    print(f"[worker-{self.wid}] job {job_id} failed rc={r.returncode}: {err.strip()}")

                    # ✅ Record failure event
                    record("complete", job_id=job_id, status="failed")

                    attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                    if attempts >= maxr:
                        self.storage.move_to_dead(job_id, last_error=err[:1000])
                        print(f"[worker-{self.wid}] moved {job_id} to DLQ")

                        # ✅ Record DLQ move
                        record("complete", job_id=job_id, status="dead")
                    else:
                        backoff = (self.cfg.get("backoff_base", 2) ** attempts)
                        print(f"[worker-{self.wid}] sleeping backoff {backoff}s for job {job_id} before next retry")
                        slept = 0
                        while slept < backoff:
                            if self._should_stop():
                                print(f"[worker-{self.wid}] stop requested during backoff; exiting gracefully.")
                                return
                            time.sleep(0.5)
                            slept += 0.5

            except subprocess.TimeoutExpired:
                print(f"[worker-{self.wid}] job {job_id} timed out")
                attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                if attempts >= maxr:
                    self.storage.move_to_dead(job_id, last_error="timeout")
                    print(f"[worker-{self.wid}] moved {job_id} to DLQ (timeout)")
                    record("complete", job_id=job_id, status="timeout")

            except Exception as e:
                print(f"[worker-{self.wid}] exception running job {job_id}: {e}")
                attempts, maxr = self.storage.increment_attempts_and_backoff(job_id)
                if attempts >= maxr:
                    self.storage.move_to_dead(job_id, last_error=str(e))
                    record("complete", job_id=job_id, status="error")

    def _should_stop(self):
        return os.path.exists(STOP_FLAG)


class WorkerManager:
    def __init__(self, db_path, count=1):
        self.storage = JobStorage(db_path)
        self.count = max(1, count)
        self.workers = []

    def run_forever(self):
        for i in range(self.count):
            w = Worker(self.storage, wid=i + 1)
            w.start()
            self.workers.append(w)
        while True:
            time.sleep(0.5)
            if os.path.exists(STOP_FLAG):
                print("Stop flag detected; requesting workers to stop gracefully")
                break
            alive = any(t.is_alive() for t in self.workers)
            if not alive:
                print("All workers finished")
                break
        for t in self.workers:
            t.join(timeout=10)

    def stop(self):
        open(STOP_FLAG, "w").write("stop")
