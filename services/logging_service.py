"""
Structured JSON logging service.

Provides file-based structured logging alongside the existing console output.
Logs are written to data/logs/ with daily rotation.
"""

import json
import logging
import os
import sys
from datetime import datetime, UTC
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_DIR = Path(os.getenv("LOG_DIR", str(Path(__file__).parent.parent / "data" / "logs")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB per file
BACKUP_COUNT = 5                  # Keep 5 rotated files


class JSONFormatter(logging.Formatter):
    """Format log records as JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "job_id"):
            log_entry["jobId"] = record.job_id
        if hasattr(record, "item_id"):
            log_entry["itemId"] = record.item_id
        if hasattr(record, "platform"):
            log_entry["platform"] = record.platform
        if hasattr(record, "event"):
            log_entry["event"] = record.event

        # Add exception info
        if record.exc_info and record.exc_info[1]:
            log_entry["error"] = str(record.exc_info[1])
            log_entry["traceback"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging():
    """
    Configure structured logging for the application.
    Sets up both console (human-readable) and file (JSON) handlers.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Remove existing handlers
    root.handlers.clear()

    # Console handler — human-readable
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root.addHandler(console)

    # File handler — structured JSON
    log_file = LOG_DIR / "scraper.log"
    file_handler = RotatingFileHandler(
        str(log_file),
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JSONFormatter())
    root.addHandler(file_handler)

    # Error-only file
    error_file = LOG_DIR / "errors.log"
    error_handler = RotatingFileHandler(
        str(error_file),
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JSONFormatter())
    root.addHandler(error_handler)

    # Quiet noisy libraries
    for lib in ("httpx", "httpcore", "asyncio", "playwright"):
        logging.getLogger(lib).setLevel(logging.WARNING)

    logging.info("Structured logging initialized", extra={"event": "logging.init"})


def get_scraper_logger(job_id: str = None) -> logging.LoggerAdapter:
    """Get a logger with job context."""
    logger = logging.getLogger("scraper")
    if job_id:
        return logging.LoggerAdapter(logger, {"job_id": job_id})
    return logging.LoggerAdapter(logger, {})


def get_recent_logs(limit: int = 100, level: str = None) -> list[dict]:
    """Read recent log entries from the log file."""
    log_file = LOG_DIR / "scraper.log"
    if not log_file.exists():
        return []

    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    entries = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            if level and entry.get("level") != level.upper():
                continue
            entries.append(entry)
            if len(entries) >= limit:
                break
        except json.JSONDecodeError:
            continue

    return entries


def get_log_stats() -> dict:
    """Get logging statistics."""
    if not LOG_DIR.exists():
        return {"directory": str(LOG_DIR), "files": 0, "totalSizeMB": 0}

    files = list(LOG_DIR.glob("*.log*"))
    total_size = sum(f.stat().st_size for f in files if f.is_file())

    return {
        "directory": str(LOG_DIR),
        "files": len(files),
        "totalSizeMB": round(total_size / (1024 * 1024), 2),
        "logLevel": LOG_LEVEL,
    }
