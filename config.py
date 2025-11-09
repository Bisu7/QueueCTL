"""
Simple JSON-backed configuration.
"""
import json
import os

DEFAULTS = {
    "max_retries": 3,
    "backoff_base": 2,
    "worker_poll_interval": 1.0,  # seconds
    "job_timeout": 60,  # seconds default timeout
}

CFG_PATH = os.path.join(os.path.dirname(__file__), "queuectl_config.json")

class Config:
    def __init__(self):
        self.path = CFG_PATH
        if not os.path.exists(self.path):
            self._write(DEFAULTS)
        self._load()

    def _load(self):
        with open(self.path, "r") as f:
            self.data = json.load(f)

    def _write(self, d):
        with open(self.path, "w") as f:
            json.dump(d, f, indent=2)

    def get(self, key, default=None):
        return self.data.get(key, DEFAULTS.get(key, default))

    def set(self, key, val):
        self.data[key] = val
        self._write(self.data)

    def all(self):
        return dict(self.data)
