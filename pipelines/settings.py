from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class CrawlerSettings(BaseSettings):
    """
    Environment-driven settings for crawler + sentiment inference.
    """

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    # ---- Crawling ----
    inven_board_id: int = Field(default=5558, alias="INVEN_BOARD_ID")
    inven_board_base_url: str = Field(
        default="https://m.inven.co.kr/board/lostark/5558",
        alias="INVEN_BOARD_BASE_URL",
    )

    request_timeout_sec: float = Field(default=15.0, alias="INVEN_REQUEST_TIMEOUT_SEC")
    request_delay_sec: float = Field(default=0.8, alias="INVEN_REQUEST_DELAY_SEC")

    max_list_pages: int = Field(default=1, alias="INVEN_MAX_LIST_PAGES")
    max_posts_per_run: int = Field(default=30, alias="INVEN_MAX_POSTS_PER_RUN")

    max_retries: int = Field(default=3, alias="INVEN_MAX_RETRIES")
    backoff_base_sec: float = Field(default=1.0, alias="INVEN_BACKOFF_BASE_SEC")
    backoff_max_sec: float = Field(default=20.0, alias="INVEN_BACKOFF_MAX_SEC")

    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        alias="INVEN_USER_AGENT",
    )

    dump_html_on_empty: bool = Field(default=True, alias="INVEN_DUMP_HTML_ON_EMPTY")
    dump_html_path: str = Field(default="debug_board_list.html", alias="INVEN_DUMP_HTML_PATH")

    # ---- Sentiment inference ----
    # You said you already saved the fine-tuned model from notebook.
    # Default assumes repo root has ./fine_tuned_model
    sentiment_model_path: str = Field(default="./fine_tuned_model", alias="SENTIMENT_MODEL_PATH")
    sentiment_model_version: str = Field(default="kcbert-finetuned-v1", alias="SENTIMENT_MODEL_VERSION")

    # text_used: "title" or "title+content"
    sentiment_text_used: str = Field(default="title", alias="SENTIMENT_TEXT_USED")

    sentiment_batch_size: int = Field(default=16, alias="SENTIMENT_BATCH_SIZE")
    sentiment_max_length: int = Field(default=256, alias="SENTIMENT_MAX_LENGTH")

    # Softmax thresholding (optional): if max prob below this -> neutral
    sentiment_neutral_floor: float = Field(default=0.0, alias="SENTIMENT_NEUTRAL_FLOOR")

    # Device: "auto" | "cpu" | "cuda"
    sentiment_device: str = Field(default="auto", alias="SENTIMENT_DEVICE")


def load_settings() -> CrawlerSettings:
    return CrawlerSettings()
