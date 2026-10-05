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

Each stderr line is also its own ERROR record, so an exception logged with its
traceback ("Exception in ASGI application") became ~150 records per failed
request and as many alert groups (SCRUM-6632). An ERROR record is therefore one
stderr line, naming the exception and the innermost frame in our own code; the
full traceback goes to stdout (INFO), next to it in the viewer.
"""
import logging
import sys
import sysconfig
import traceback

from gunicorn.glogging import Logger

# The level at or above which a record still goes to stderr.
STDERR_LEVEL = logging.ERROR
# Enough for the exception and its location to fit an alert's sample.
SUMMARY_CHARS = 300

_LIBRARY_DIRS = tuple({sysconfig.get_paths()[k] for k in ("stdlib", "platstdlib", "purelib", "platlib")})


class _BelowLevelFilter(logging.Filter):
    """Pass only records strictly below the given level."""

    def __init__(self, level: int) -> None:
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.level


def _has_exception(record: logging.LogRecord) -> bool:
    """True if the record carries an actual exception. logger.exception() or
    exc_info=True outside an except block gives (None, None, None), which is
    truthy but has nothing to show."""
    return record.exc_info is not None and record.exc_info[0] is not None


class _TracebackFilter(logging.Filter):
    """Pass ERROR-and-above records that carry an exception: their full traceback
    goes to stdout, alongside the one-line version on stderr."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= STDERR_LEVEL and _has_exception(record)


def _exception_summary(exc_info) -> str:
    """'module.Type: first line of the message (file.py:42 in func)'."""
    exc_type, exc, tb = exc_info
    name = exc_type.__qualname__ if exc_type.__module__ == "builtins" \
        else f"{exc_type.__module__}.{exc_type.__qualname__}"
    message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    frames = traceback.extract_tb(tb)
    # The innermost frame outside the standard library and installed packages,
    # i.e. where our own code raised; fall back to the innermost frame.
    own = [f for f in frames if not f.filename.startswith(_LIBRARY_DIRS)]
    where = (own or frames)[-1] if frames else None
    location = f" ({where.filename.rsplit('/', 1)[-1]}:{where.lineno} in {where.name})" if where else ""
    summary = f"{name}: {message}" if message else name
    return (summary[:SUMMARY_CHARS] + location)


class _TracebackOnlyFormatter(logging.Formatter):
    """Just the traceback. The record's own line is already on stderr, and the
    alerter matches "[ERROR]" text on stdout too, so repeating it here would make
    every failure two alert groups."""

    def format(self, record: logging.LogRecord) -> str:
        return self.formatException(record.exc_info) if record.exc_info else ""


class _OneLineFormatter(logging.Formatter):
    """Render a record as a single line: its own text with newlines collapsed,
    plus a summary of the exception instead of the traceback."""

    def __init__(self, base: logging.Formatter) -> None:
        super().__init__()
        self.base = base

    def format(self, record: logging.LogRecord) -> str:
        plain = logging.makeLogRecord(record.__dict__)
        plain.exc_info, plain.exc_text, plain.stack_info = None, None, None
        line = " ".join(self.base.format(plain).split())
        if _has_exception(record):
            line = f"{line} | {_exception_summary(record.exc_info)}"
        return line


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
        tracebacks = logging.StreamHandler(sys.stdout)
        tracebacks.addFilter(_TracebackFilter())
        errors = logging.StreamHandler(sys.stderr)
        errors.setLevel(STDERR_LEVEL)
        for handler in (routine, tracebacks, errors):
            handler.setFormatter(original.formatter)
            handler._gunicorn = True  # type: ignore[attr-defined]
            handler._split_stream = True  # type: ignore[attr-defined]
        tracebacks.setFormatter(_TracebackOnlyFormatter())
        errors.setFormatter(_OneLineFormatter(original.formatter or logging.Formatter()))

        # Replace in place: uvicorn's worker aliases this very list for
        # uvicorn.error, so it must be the same object, not a new one.
        handlers = self.error_log.handlers
        handlers[handlers.index(original)] = routine
        handlers.append(tracebacks)
        handlers.append(errors)
