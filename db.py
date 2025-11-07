import sqlite3
import threading
from datetime import datetime
from contextlib import contextmanager

DBPath = "queue.db"
_lock = threading.Lock()


def _log(msg:str):
    print(f"datetime.now()")

@contextmanager
def getConn():
    with _lock:
        conn = sqlite3.connect(DBPath, timeout=30,isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('PRAGMA journal_mode=WAL;')
            yield conn
        finally:
            conn.close()

def init_DB():
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                maxRetries INTEGER NOT NULL DEFAULT 3,
                baseBackoff INTEGER NOT NULL DEFAULT 2,
                createdAt TEXT NOT NULL,
                updatedAt TEXT NOT NULL,
                nextRunAt TEXT NOT NULL,
                lockedBy TEXT,
                timeout INTEGER,
                outputLog TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS config(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.commit()
        _log("Database Initailized")

def execute(query: str, params: tuple = ()):
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute('BEGIN')
        cur.execute(query,params)
        conn.commit()
        _log(f"query : {query.split()[0]}")

def fetchAll(query: str, params: tuple = ()):
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute(query,params)
        rows = cur.fetchall()
        return rows
    
def fetchOne(query: str, params: tuple = ()):
    with getConn as conn:
        cur = conn.cursor()
        cur.execute(query,params)
        row = cur.fetchone()
        return row if row else None
    