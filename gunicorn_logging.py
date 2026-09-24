"""Gunicorn logger that puts only real errors on stderr (SCRUM-6248).

Gunicorn writes its whole error log -- which, despite the name, carries every
"Booting worker", "Worker exiting" and "Started server process" line -- to a
single stream, stderr when errorlog is '-'. The Docker GELF driver derives
severity from the stream alone (stdout -> INFO, stderr -> ERROR), so every
routine worker recycle arrived in the log store labelled ERROR and buried the
real failures in the viewer.

This splits that one handler in two: records up to WARNING go to stdout, ERROR
and CRITICAL stay on stderr. Only the error log is touched; the access log is
already on stdout and the root logger is left to the application's own
logging.conf. uvicorn's worker shares these handlers for its uvicorn.error
logger, so its lines are routed the same way.
"""
import logging
import sys

from gunicorn.glogging import Logger

# The level at or above which a record still goes to stderr.
STDERR_LEVEL = logging.ERROR


class _BelowLevelFilter(logging.Filter):
    """Pass only records strictly below the given level."""

    def __init__(self, level: int) -> None:
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.level


class SplitStreamLogger(Logger):
    """Gunicorn's Logger with its stderr error handler split by level."""

    def setup(self, cfg) -> None:
        # setup() runs again on SIGHUP, and the base class only removes one
        # handler of its own before adding a new one -- drop ours first or each
        # reload would duplicate every line.
        for handler in list(self.error_log.handlers):
            if getattr(handler, "_split_stream", False):
                self.error_log.removeHandler(handler)

        super().setup(cfg)

        if cfg.errorlog != "-":
            return  # a log file: stream severity does not apply
        original = next(
            (h for h in self.error_log.handlers
             if getattr(h, "_gunicorn", False) and type(h) is logging.StreamHandler
             and h.stream is sys.stderr),
            None,
        )
        if original is None:
            return

        routine = logging.StreamHandler(sys.stdout)
        routine.addFilter(_BelowLevelFilter(STDERR_LEVEL))
        errors = logging.StreamHandler(sys.stderr)
        errors.setLevel(STDERR_LEVEL)
        for handler in (routine, errors):
            handler.setFormatter(original.formatter)
            handler._gunicorn = True  # type: ignore[attr-defined]
            handler._split_stream = True  # type: ignore[attr-defined]

        # Replace in place: uvicorn's worker aliases this very list for
        # uvicorn.error, so it must be the same object, not a new one.
        handlers = self.error_log.handlers
        handlers[handlers.index(original)] = routine
        handlers.append(errors)
