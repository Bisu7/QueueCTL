from dataclasses import dataclass
from typing import Optional

@dataclass
class Job:
    id: str
    command: str
    state: str = "pending"
    attempts: int = 0
    max_retries: int = 3
    created_at: str = None
    updated_at: str = None
    last_error: Optional[str] = None
