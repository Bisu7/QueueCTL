import uuid
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def gen_id(prefix="job"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
