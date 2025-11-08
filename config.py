import os
from typing import Any
from database import ConfigEntry, get_config_session

class Config:
    """
    Manages system configuration persisted in the database.
    
    Default values are defined here.
    """
    
    DEFAULTS = {
        'max-retries': 3,
        'backoff-base': 2, # Exponential backoff: delay = base ^ attempts
        'log-level': 'INFO',
        'worker-timeout': 3600 # Max seconds a job can run before being considered failed
    }
    
    ALLOWED_KEYS = list(DEFAULTS.keys())

    @staticmethod
    def _validate(key: str, value: Any):
        """Perform type and value validation."""
        if key == 'max-retries':
            val = int(value)
            if val < 0: raise ValueError("max-retries must be non-negative.")
            return val
        if key == 'backoff-base':
            val = int(value)
            if val < 1: raise ValueError("backoff-base must be a positive integer.")
            return val
        if key == 'log-level' and value not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            raise ValueError("log-level must be DEBUG, INFO, WARNING, or ERROR.")
        return value

    @staticmethod
    def get(key: str) -> Any:
        """Retrieve a configuration value."""
        if key not in Config.ALLOWED_KEYS:
            return None

        session = get_config_session()
        try:
            entry: ConfigEntry = session.query(ConfigEntry).filter_by(key=key).first()
            if entry:
                # Convert back to the appropriate type if necessary
                value = entry.value
                if key in ['max-retries', 'backoff-base', 'worker-timeout']:
                    return int(value)
                return value
            
            # Return default if not found
            return Config.DEFAULTS[key]
        finally:
            session.close()

    @staticmethod
    def set(key: str, value: Any):
        """Set and persist a configuration value."""
        if key not in Config.ALLOWED_KEYS:
            raise ValueError(f"Unknown configuration key: {key}")
        
        # Validate and convert value
        validated_value = Config._validate(key, value)
        
        session = get_config_session()
        try:
            entry: ConfigEntry = session.query(ConfigEntry).filter_by(key=key).first()
            if entry:
                entry.value = str(validated_value)
            else:
                entry = ConfigEntry(key=key, value=str(validated_value))
                session.add(entry)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

# Initialize with defaults if needed (handled by get/set logic)