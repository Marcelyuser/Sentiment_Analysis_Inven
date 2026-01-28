from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpConfig:
    timeout_sec: float
    delay_sec: float
    max_retries: int
    backoff_base_sec: float
    backoff_max_sec: float
    user_agent: str


class HttpClient:
    """
    Thin HTTP client wrapper:
    - Timeout
    - Rate limiting (fixed delay + small jitter)
    - Retry with exponential backoff
    - Logs meaningful failures

    This client does NOT attempt to bypass protections.
    """

    def __init__(self, config: HttpConfig, session: Optional[requests.Session] = None):
        self._cfg = config
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "User-Agent": self._cfg.user_agent,
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            }
        )

    def get_text(self, url: str) -> str:
        """
        GET an URL and return response body as text.

        Raises:
            requests.HTTPError: non-2xx responses after retries
            requests.RequestException: network errors after retries
        """
        self._rate_limit()

        last_exc: Exception | None = None
        for attempt in range(self._cfg.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=self._cfg.timeout_sec)
                # If blocked, do not hammer. Backoff and retry a limited number of times.
                if resp.status_code in (403, 429):
                    raise requests.HTTPError(
                        f"Blocked or rate-limited: status={resp.status_code} url={url}",
                        response=resp,
                    )
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp.text
            except (requests.HTTPError, requests.RequestException) as e:
                last_exc = e
                if attempt >= self._cfg.max_retries:
                    logger.error("HTTP GET failed after retries: url=%s err=%s", url, e)
                    raise
                sleep_sec = self._compute_backoff(attempt)
                logger.warning(
                    "HTTP GET failed (retrying): attempt=%s url=%s sleep=%.2fs err=%s",
                    attempt + 1,
                    url,
                    sleep_sec,
                    e,
                    )
                time.sleep(sleep_sec)

        # Should not reach here
        assert last_exc is not None
        raise last_exc

    def _rate_limit(self) -> None:
        # Fixed delay + jitter (avoid bursty patterns)
        jitter = random.uniform(0.0, 0.25)
        time.sleep(self._cfg.delay_sec + jitter)

    def _compute_backoff(self, attempt: int) -> float:
        # Exponential backoff with cap + jitter
        base = self._cfg.backoff_base_sec * (2**attempt)
        capped = min(base, self._cfg.backoff_max_sec)
        return capped + random.uniform(0.0, 0.5)
