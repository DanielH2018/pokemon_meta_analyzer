"""Logging utilities for structured logging and flight recording."""

import contextvars
import logging
import sys
import time

import structlog

# region Logging Setup

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

# --- 1. Context Variables ---

# This holds the logger used for PRINTING to the console
_logger_ctx = contextvars.ContextVar("logger_ctx")

# This holds the data accumulating for the FINAL SUMMARY (Hidden from console)
_flight_data_ctx = contextvars.ContextVar("flight_data_ctx")


# --- 2. The Enhanced Context Manager ---
class LoggingContext:
    """A context manager for logging script execution details."""

    def __init__(self, script_name: str, **initial_vars):
        """Initializes the LoggingContext."""
        self.script_name = script_name
        self.initial_vars = initial_vars
        self.start_time = 0.0
        self.token_logger = None
        self.token_data = None

    def __enter__(self):
        """Sets up logging context and starts timing."""
        self.start_time = time.time()

        # 1. Initialize the Silent Flight Data
        # Start with the initial vars (like user_id, job_id)
        current_data = self.initial_vars.copy()
        self.token_data = _flight_data_ctx.set(current_data)

        # 2. Initialize the Active Logger
        # We ONLY bind the script name so the console logs stay clean
        logger = structlog.get_logger().bind(script=self.script_name)
        self.token_logger = _logger_ctx.set(logger)

        return logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finalizes logging context and prints summary."""
        duration = (time.time() - self.start_time) * 1000

        # 1. Gather all the data into one dictionary
        final_data = _flight_data_ctx.get()
        summary_payload = {
            "status": "failed" if exc_type else "success",
            "duration_ms": round(duration, 2),
            **final_data,
        }

        # 2. Add Error Details if present
        if exc_type:
            summary_payload["error"] = str(exc_val)
            summary_payload["error_type"] = exc_type.__name__

        # 3. HUMAN VIEW: If running locally, print a pretty block
        #    We use json.dumps with indent=4 to make it readable.
        if sys.stdout.isatty():
            import json

            print("\n" + "=" * 50)
            print("FLIGHT RECORDER SUMMARY")
            print("=" * 50)
            # default=str handles objects like datetime that JSON can't natively serialize
            print(json.dumps(summary_payload, indent=4, default=str))
            print("=" * 50 + "\n")

        # 4. MACHINE RECORD: Log the structured event
        #    This ensures the data is still searchable/parsable in your logs
        get_logger().info("flight_recorder_summary", **summary_payload)

        # Cleanup
        if self.token_logger:
            _logger_ctx.reset(self.token_logger)
        if self.token_data:
            _flight_data_ctx.reset(self.token_data)

        return False


# --- 3. Helper Functions ---


def setup_logging(debug_mode: bool = False):
    """Sets up structlog logging configuration."""
    if sys.stdout.isatty():
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    min_level = logging.DEBUG if debug_mode else logging.INFO

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(min_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger() -> structlog.BoundLogger:
    """Retrieves the current logger from context or creates a new one."""
    try:
        return _logger_ctx.get()
    except LookupError:
        return structlog.get_logger()


def add_context(**kwargs):
    """Silently adds data to the Flight Recorder.

    This data will NOT appear in console logs, only in the final summary.
    """
    try:
        # Get the current data dictionary
        current_data = _flight_data_ctx.get()

        # Create a copy to ensure context safety (immutability pattern)
        new_data = current_data.copy()
        new_data.update(kwargs)

        # Save it back to the context
        _flight_data_ctx.set(new_data)

    except LookupError:
        # If called outside the context, we just ignore it (or you could log a warning)
        pass


# endregion
