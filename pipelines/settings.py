from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class CrawlerSettings(BaseSettings):
    """
    Environment-driven settings for crawler behavior.

    All defaults are intentionally conservative to reduce load and avoid blocks.
    """

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    inven_board_id: int = Field(default=5861, alias="INVEN_BOARD_ID")
    inven_board_base_url: str = Field(
        default="https://m.inven.co.kr/board/lostark/5861",
        alias="INVEN_BOARD_BASE_URL",
    )

    request_timeout_sec: float = Field(default=15.0, alias="INVEN_REQUEST_TIMEOUT_SEC")
    request_delay_sec: float = Field(default=0.8, alias="INVEN_REQUEST_DELAY_SEC")

    max_list_pages: int = Field(default=1, alias="INVEN_MAX_LIST_PAGES")
    max_posts_per_run: int = Field(default=30, alias="INVEN_MAX_POSTS_PER_RUN")

    # Retry / backoff
    max_retries: int = Field(default=3, alias="INVEN_MAX_RETRIES")
    backoff_base_sec: float = Field(default=1.0, alias="INVEN_BACKOFF_BASE_SEC")
    backoff_max_sec: float = Field(default=20.0, alias="INVEN_BACKOFF_MAX_SEC")

    # User-Agent (set a descriptive one in production)
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        alias="INVEN_USER_AGENT",
    )

    # Debug helpers
    dump_html_on_empty: bool = Field(default=True, alias="INVEN_DUMP_HTML_ON_EMPTY")
    dump_html_path: str = Field(default="debug_board_list.html", alias="INVEN_DUMP_HTML_PATH")


def load_settings() -> CrawlerSettings:
    """Load settings from environment / .env."""
    return CrawlerSettings()
