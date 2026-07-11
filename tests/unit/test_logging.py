from __future__ import annotations

import logging

from jasi.logging import configure_logging


def test_http_client_request_logs_are_suppressed() -> None:
    configure_logging("INFO")

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
