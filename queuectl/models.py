import sqlite3
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class Job:
    id: str
    command: str
    state: str
    attempts: int
    max_retries: int
    created_at: str
    updated_at: str
    locked_by: Optional[str] = None
    locked_at: Optional[str] = None
    lease_expires_at: Optional[str] = None
    next_attempt_at: Optional[str] = None
    priority: int = 0

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        return cls(
            id=row["id"],
            command=row["command"],
            state=row["state"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            locked_by=row["locked_by"],
            locked_at=row["locked_at"],
            lease_expires_at=row["lease_expires_at"] if "lease_expires_at" in row.keys() else None,
            next_attempt_at=row["next_attempt_at"] if "next_attempt_at" in row.keys() else None,
            priority=row["priority"] if "priority" in row.keys() else 0,
        )

    def to_dict(self) -> dict:
        return asdict(self)
