"""Shared utilities (e.g. timing) for sindex."""

import time
from contextlib import contextmanager


@contextmanager
def timed_block(label: str):
    """
    Context manager that prints a start message, runs the block, then prints
    a completion message with elapsed time in seconds.

    Usage:
        with timed_block("Fetching metadata"):
            do_work()
        # Prints: [status] Fetching metadata...
        # Then:   [status] Fetching metadata done (1.23s)
    """
    print(f"[status] {label}...")
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"[status] {label} done ({elapsed:.2f}s)")


def timed_call(label: str, fn, *args, **kwargs):
    """
    Run a callable and print completion status with elapsed time.
    Returns the callable's return value.

    Useful for timing individual tasks (e.g. in thread pools).
    """
    start = time.perf_counter()
    try:
        return fn(*args, **kwargs)
    finally:
        elapsed = time.perf_counter() - start
        print(f"[status] {label} done ({elapsed:.2f}s)")
