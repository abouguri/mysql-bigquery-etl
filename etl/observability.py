"""Structured events deliberately exclude record payloads and exception text."""
from datetime import datetime, timezone
import json
import logging


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = dict(timestamp=datetime.now(timezone.utc).isoformat(), severity=record.levelname,
                       event=getattr(record, 'event', 'diagnostic'), message=record.getMessage())
        payload.update(getattr(record, 'fields', {}))
        return json.dumps(payload, default=str)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def event(logger, name, *, severity=logging.INFO, **fields):
    logger.log(severity, name, extra={'event': name, 'fields': fields})
