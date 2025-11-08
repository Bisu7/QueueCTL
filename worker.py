import time
import subprocess
import os
import signal
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

# Local imports
from database import Job, get_session
from config import Config
from cli_utils import get_log_dir, get_pid_file

# --- Logging Setup ---
# Workers log their activity to a file for later inspection
def setup_worker_logger(worker_pid: int):
    log_dir = get_log_dir()
    log_file = os.path.join(log_dir, f'worker_{worker_pid}.log')
    
    logging.basicConfig(
        level=Config.get('log-level'),
        format='%(asctime)s | PID %(process)d | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout) # Also log to console for immediate feedback
        ]
    )
    return logging.getLogger(__name__)

# --- Core Worker Class ---

class Worker:
    def __init__(self, pid: int):
        self.pid = pid
        self.running = True
        self.logger = setup_worker_logger(pid)
        self.logger.info(f"Worker {self.pid} started.")

        # Handle signals for graceful shutdown
        signal.signal(signal.SIGINT, self.graceful_shutdown)  # Ctrl+C
        signal.signal(signal.SIGTERM, self.graceful_shutdown) # Kill signal

    def graceful_shutdown(self, signum, frame):
        """Gracefully stop the worker."""
        self.logger.warning(f"Worker {self.pid} received signal {signum}. Shutting down gracefully...")
        self.running = False

    def get_job_to_process(self) -> Optional[Job]:
        """
        Atomically fetch a 'pending' job and change its state to 'processing'.
        
        This uses a SELECT FOR UPDATE pattern equivalent using a transaction 
        and state change to prevent other workers from picking up the same job.
        """
        session = get_session()
        try:
            # Look for a job that is 'pending' or 'failed' and past its retry_at time
            now = datetime.now(timezone.utc)
            
            # Query for a ready job: (pending) OR (failed AND retry_at is past now)
            job: Optional[Job] = session.query(Job).filter(
                Job.state.in_(['pending', 'failed']),
                (Job.retry_at == None) | (Job.retry_at <= now)
            ).order_by(Job.created_at.asc()).with_for_update().first() # Lock the row
            
            if job:
                # Atomically change state
                job.state = 'processing'
                job.process_pid = self.pid
                job.updated_at = now
                session.commit()
                self.logger.info(f"Picked up job {job.id[:8]} for processing. Attempts: {job.attempts}")
                return job
            
            session.rollback() # Release the lock if no job was found/committed
            return None
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error fetching job: {e}")
            return None
        finally:
            session.close()

    def process_job(self, job: Job):
        """Execute the job's command and update its state based on the exit code."""
        session = get_session()
        job_id = job.id[:8]

        # 1. Execute Command
        try:
            self.logger.info(f"Executing job {job_id}: '{job.command}'")
            
            # Use subprocess.run for robust command execution
            result = subprocess.run(
                job.command,
                shell=True,  # Necessary to execute compound commands like 'sleep 2 && echo done'
                capture_output=True,
                text=True,
                check=False, # Don't raise an exception on non-zero exit code
                timeout=job.timeout # Enforce job timeout
            )
            
            # 2. Check Result and Update State
            job = session.merge(job) # Re-attach job object to the session
            now = datetime.now(timezone.utc)
            
            if result.returncode == 0:
                # SUCCESS
                job.state = 'completed'
                job.process_pid = None
                job.last_error = f"Exit Code 0. Stdout: {result.stdout.strip()[:100]}"
                self.logger.info(f"Job {job_id} completed successfully.")
            else:
                # FAILURE: Check for retries
                job.attempts += 1
                job.last_error = f"Exit Code {result.returncode}. Stderr: {result.stderr.strip()[:100]}"
                
                if job.attempts <= job.max_retries:
                    # RETRY REQUIRED (FAILED)
                    backoff_base = Config.get('backoff-base')
                    delay = backoff_base ** job.attempts
                    job.retry_at = now + timedelta(seconds=delay)
                    job.state = 'failed'
                    self.logger.warning(f"Job {job_id} failed (Attempt {job.attempts}/{job.max_retries}). Retrying in {delay} seconds.")
                else:
                    # MAX RETRIES REACHED (DEAD)
                    job.state = 'dead'
                    job.process_pid = None
                    self.logger.error(f"Job {job_id} permanently failed after {job.attempts} attempts. Moved to DLQ.")
        
        except subprocess.TimeoutExpired:
            # TIMEOUT
            job.attempts += 1
            job.last_error = "Job execution timed out."
            if job.attempts <= job.max_retries:
                # RETRY
                backoff_base = Config.get('backoff-base')
                delay = backoff_base ** job.attempts
                job.retry_at = now + timedelta(seconds=delay)
                job.state = 'failed'
                self.logger.warning(f"Job {job_id} timed out (Attempt {job.attempts}/{job.max_retries}). Retrying in {delay} seconds.")
            else:
                # DLQ
                job.state = 'dead'
                job.process_pid = None
                self.logger.error(f"Job {job_id} timed out and moved to DLQ.")
                
        except Exception as e:
            # GENERAL ERROR (e.g., command not found, ORM error)
            job.attempts += 1
            job.last_error = f"Internal worker error: {str(e)[:100]}"
            if job.attempts <= job.max_retries:
                # RETRY
                job.state = 'failed'
                self.logger.error(f"Job {job_id} internal error: {e}. Retrying.")
            else:
                # DLQ
                job.state = 'dead'
                job.process_pid = None
                self.logger.error(f"Job {job_id} internal error: {e}. Moved to DLQ.")
            
        finally:
            try:
                job.updated_at = datetime.now(timezone.utc)
                session.commit()
            except Exception as e:
                # Catastrophic DB error (e.g., connection lost). Job will eventually be retried/cleaned up.
                self.logger.critical(f"Failed to commit job status update for {job_id}: {e}")
                session.rollback()
            finally:
                session.close()

    def run(self):
        """Main worker loop."""
        while self.running:
            job = self.get_job_to_process()
            
            if job:
                self.process_job(job)
            else:
                # Sleep briefly if no jobs are available
                time.sleep(1)
        
        # Cleanup when the loop exits
        self.logger.warning(f"Worker {self.pid} finished execution loop and is exiting.")
        os.remove(get_pid_file(self.pid))


# --- Worker Management Functions ---

def start_worker_process(pid_file_path: str):
    """
    Function executed by the child process to start the worker instance.
    """
    current_pid = os.getpid()
    
    # Write PID to file for tracking
    try:
        with open(pid_file_path, 'w') as f:
            f.write(str(current_pid))
    except IOError as e:
        print(f"Error: Could not write PID to file {pid_file_path}. {e}", file=sys.stderr)
        sys.exit(1)

    # Initialize and run the worker
    try:
        worker = Worker(current_pid)
        worker.run()
    except Exception as e:
        print(f"FATAL: Worker {current_pid} failed to run: {e}", file=sys.stderr)
    finally:
        # Ensure PID file is removed on clean exit
        if os.path.exists(pid_file_path):
            os.remove(pid_file_path)

# In worker.py, replace the existing start_workers function

def start_workers(count: int):
    """Starts worker processes using subprocess.Popen for Windows compatibility."""
    log_dir = get_log_dir(create=True)
    
    # Use the Python interpreter's path
    python_executable = sys.executable 
    
    for i in range(count):
        # We need to run the worker script itself as a separate process
        # The worker needs a way to distinguish between the CLI and a worker call
        
        # A simple way: pass a special argument to the worker script
        command = [python_executable, os.path.abspath(__file__), '__worker__']
        
        # Start the worker in the background
        # We redirect stdout/stderr to files to prevent I/O blocking
        # And use DETACHED_PROCESS flag for true background operation in Windows
        
        # Note: This is a simplified Windows approach. 
        # PID tracking and graceful shutdown via signals (SIGTERM)
        # will be unreliable or require the use of `signal.CTRL_BREAK_EVENT` 
        # or similar Windows-specific methods.
        
        subprocess.Popen(
            command, 
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        )
        print(f"Windows Worker {i+1} initiated (PID not tracked reliably).")
        

def stop_workers() -> bool:
    """Sends SIGTERM to all running workers for graceful shutdown."""
    pids = get_worker_pids()
    if not pids:
        return False
        
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            # If the process already died, clean up the PID file
            pid_file = get_pid_file(pid)
            if os.path.exists(pid_file):
                os.remove(pid_file)
    
    return True