"""Per-request context for correlating logs and LangSmith traces with login session_id."""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator, Optional

_request_session_id: ContextVar[str] = ContextVar("request_session_id", default="")


def get_request_session_id() -> str:
    return str(_request_session_id.get() or "").strip()


def set_request_session_id(session_id: str) -> Token:
    return _request_session_id.set(str(session_id or "").strip())


def reset_request_session_id(token: Token) -> None:
    _request_session_id.reset(token)


@contextmanager
def bind_request_session(session_id: str) -> Iterator[None]:
    """Bind session_id for the current request (LangSmith + LLM child traces)."""
    sid = str(session_id or "").strip()
    token = set_request_session_id(sid)
    prev_langchain_session = os.environ.get("LANGCHAIN_SESSION")
    if sid:
        os.environ["LANGCHAIN_SESSION"] = sid
    try:
        yield
    finally:
        reset_request_session_id(token)
        if prev_langchain_session is None:
            os.environ.pop("LANGCHAIN_SESSION", None)
        else:
            os.environ["LANGCHAIN_SESSION"] = prev_langchain_session
