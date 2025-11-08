import uuid
import os
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import event
from sqlalchemy.engine import Engine

# Configuration
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queue.db')
ENGINE = create_engine(f'sqlite:///{DB_PATH}')
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

# --- SQLite Connection Optimizations ---
# Enforce foreign key constraints and journaling mode for better concurrency
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL") # Write-Ahead Logging for better concurrency
    cursor.close()

# --- Job Model ---

class Job(Base):
    __tablename__ = 'jobs'

    # Primary key, auto-generated unique ID
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    
    # Job fields
    command = Column(String, nullable=False)
    state = Column(String, default="pending", nullable=False) # pending, processing, completed, failed, dead
    attempts = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    
    # Timeouts (Future feature, but good to include)
    timeout = Column(Integer, default=300) # seconds

    # Status/Logging fields
    last_error = Column(String, nullable=True)
    process_pid = Column(Integer, nullable=True)
    
    # Timestamps (Use timezone aware datetime for robustness)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    retry_at = Column(DateTime(timezone=True), nullable=True) # Used for backoff scheduling

    def __repr__(self):
        return f"<Job(id='{self.id[:8]}', state='{self.state}', attempts={self.attempts}, command='{self.command[:20]}...')>"

# --- Utility Functions ---

def initialize_db():
    """Create the database and tables if they don't exist."""
    Base.metadata.create_all(bind=ENGINE)

def get_session():
    """Returns a new session object."""
    return SessionLocal()

# --- Config Table for Persistence ---
class ConfigEntry(Base):
    __tablename__ = 'config'
    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)

def get_config_session():
    """Returns a new session object for config operations."""
    return SessionLocal()