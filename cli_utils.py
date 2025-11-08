import os
import sys
import json
from typing import List, Dict, Any, Optional

from database import Job, get_session
from sqlalchemy import func
from prettytable import PrettyTable

# Emojis for status display
STATE_EMOJIS = {
    'pending': '⏳',
    'processing': '⚙️',
    'completed': '✅',
    'failed': '❌',
    'dead': '⚰️'
}

# --- File/PID Management ---

def get_base_dir() -> str:
    """Returns the directory where the queuectl script is located."""
    return os.path.dirname(os.path.abspath(__file__))

def get_pid_dir(create: bool = False) -> str:
    """Returns the directory for PID files."""
    pid_dir = os.path.join(get_base_dir(), '.queuectl_pids')
    if create and not os.path.exists(pid_dir):
        os.makedirs(pid_dir)
    return pid_dir

def get_log_dir(create: bool = False) -> str:
    """Returns the directory for worker log files."""
    log_dir = os.path.join(get_base_dir(), '.queuectl_logs')
    if create and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir

def get_pid_file(pid: int) -> str:
    """Returns the path to a specific PID file."""
    return os.path.join(get_pid_dir(create=True), f'{pid}.pid')

def get_worker_pids() -> List[int]:
    """Reads all active PID files and returns a list of worker PIDs."""
    pid_dir = get_pid_dir()
    if not os.path.exists(pid_dir):
        return []

    pids = []
    for filename in os.listdir(pid_dir):
        if filename.endswith('.pid'):
            try:
                with open(os.path.join(pid_dir, filename), 'r') as f:
                    pid = int(f.read().strip())
                    # Basic check if process is still running
                    if is_pid_active(pid):
                        pids.append(pid)
                    else:
                        # Clean up stale PID file
                        os.remove(os.path.join(pid_dir, filename))
            except Exception:
                # Ignore corrupt files
                pass
    return pids

def check_workers_active() -> int:
    """Returns the count of active worker processes."""
    return len(get_worker_pids())

def is_pid_active(pid: int) -> bool:
    """Check if a process with a given PID is running (Unix/Linux compatible)."""
    if pid < 0: return False
    try:
        os.kill(pid, 0) # Signal 0 checks if the process is running
    except OSError:
        return False
    return True

# --- Database Utilities ---

def get_job_summary() -> Dict[str, int]:
    """Retrieves a count of jobs for each state."""
    session = get_session()
    try:
        results = session.query(Job.state, func.count(Job.id)).group_by(Job.state).all()
        summary = {state: count for state, count in results}
        
        # Ensure all states are represented for a complete summary
        full_summary = {state: summary.get(state, 0) for state in STATE_EMOJIS.keys()}
        return full_summary
    finally:
        session.close()

# --- CLI Formatting ---

def print_job_table(headers: List[str], data: List[List[Any]]) -> str:
    """Prints a nicely formatted table using PrettyTable."""
    table = PrettyTable()
    table.field_names = headers
    for row in data:
        table.add_row(row)
    
    table.align = 'l'
    return table.get_string()