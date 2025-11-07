import subprocess
from datetime import datetime, timezone


def run_command(command: str, timeout: int | None = None) -> dict:
    start_time = datetime.now(timezone.utc)
    result = {
        "exit_code": None,
        "stdout": "",
        "stderr": "",
        "start_at": start_time.isoformat(),
        "end_at": None,
        "duration_seconds": None
    }

    try:
        process = subprocess.run(
            command,
            shell=True,                   
            capture_output=True,           
            text=True,                     
            timeout=timeout                
        )

        end_time = datetime.now(timezone.utc)
        result["exit_code"] = process.returncode
        result["stdout"] = process.stdout.strip()
        result["stderr"] = process.stderr.strip()
        result["end_at"] = end_time.isoformat()
        result["duration_seconds"] = (end_time - start_time).total_seconds()

    except subprocess.TimeoutExpired as e:
        end_time = datetime.now(timezone.utc)
        result.update({
            "exit_code": -1,
            "stdout": e.stdout or "",
            "stderr": f"Command timed out after {timeout} seconds.",
            "end_at": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds()
        })

    except FileNotFoundError as e:
        # Command not found 
        end_time = datetime.now(timezone.utc)
        result.update({
            "exit_code": 127,
            "stderr": f"Command not found: {str(e)}",
            "end_at": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds()
        })

    except Exception as e:
        #unexpected errors
        end_time = datetime.now(timezone.utc)
        result.update({
            "exit_code": 1,
            "stderr": f"Execution error: {str(e)}",
            "end_at": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds()
        })

    return result


