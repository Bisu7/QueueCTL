import sqlite3
from typing import Optional

def get_config(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM config WHERE key = ?;", (key,)).fetchone()
    if row is None:
        return default
    return row["value"]

def set_config(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?);",
        (key, value)
    )
    conn.commit()
