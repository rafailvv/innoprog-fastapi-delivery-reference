from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from fastapi import FastAPI


_MISSING = object()


@contextmanager
def dependency_override(
    app: FastAPI,
    original: Callable[..., Any],
    replacement: Callable[..., Any],
) -> Iterator[None]:
    """Install one override and restore exactly the previous mapping entry."""
    previous = app.dependency_overrides.get(original, _MISSING)
    app.dependency_overrides[original] = replacement
    try:
        yield
    finally:
        if previous is _MISSING:
            app.dependency_overrides.pop(original, None)
        else:
            app.dependency_overrides[original] = previous
