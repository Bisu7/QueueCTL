# config.py
from db import getConn

DEFAULT_CONFIG = {
    "max_retries": "3",
    "base_backoff": "2",
    "poll_interval": "1"
}

def init_config():
    """Initialize config table with default values if not already set."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        for key, value in DEFAULT_CONFIG.items():
            cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

def get_config(key=None):
    """Get a specific config key or all config values as dict."""
    with getConn() as conn:
        cur = conn.cursor()
        if key:
            cur.execute("SELECT value FROM config WHERE key=?", (key,))
            row = cur.fetchone()
            return row[0] if row else DEFAULT_CONFIG.get(key)
        else:
            cur.execute("SELECT key, value FROM config")
            rows = cur.fetchall()
            cfg = {k: v for k, v in rows}
            # Fill missing defaults
            for k, v in DEFAULT_CONFIG.items():
                cfg.setdefault(k, v)
            return cfg

def set_config(key, value):
    """Set or update a configuration value."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()

def reset_config():
    """Reset configuration table to defaults."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM config")
        for k, v in DEFAULT_CONFIG.items():
            cur.execute("INSERT INTO config (key, value) VALUES (?, ?)", (k, v))
        conn.commit()
