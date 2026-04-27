from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.crawl_state_store import CrawlStateStore
from pipelines.http_client import HttpClient, HttpConfig
from pipelines.inven_crawler import InvenCrawler
from pipelines.models import BoardPost, BoardPostRef
from pipelines.sentiment_model import SentimentModel, SentimentModelConfig
from pipelines.sentiment_pipeline import AnalyzedPost, analyze_posts
from pipelines.settings import load_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BoardConfig:
    board_id: int
    board_name: str
    board_base_url: str
    enabled: bool = True


def load_board_configs(path: str) -> list[BoardConfig]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))

    boards: list[BoardConfig] = []
    for raw in data.get("boards", []):
        board = BoardConfig(
            board_id=int(raw["board_id"]),
            board_name=str(raw.get("board_name") or raw["board_id"]),
            board_base_url=str(raw["board_base_url"]),
            enabled=bool(raw.get("enabled", True)),
        )

        if board.enabled:
            boards.append(board)

    if not boards:
        raise ValueError(f"No enabled boards found in {path}")

    return boards


def build_http_client() -> HttpClient:
    settings = load_settings()

    return HttpClient(
        HttpConfig(
            timeout_sec=settings.request_timeout_sec,
            delay_sec=settings.request_delay_sec,
            max_retries=settings.max_retries,
            backoff_base_sec=settings.backoff_base_sec,
            backoff_max_sec=settings.backoff_max_sec,
            user_agent=settings.user_agent,
        )
    )


def build_sentiment_model() -> SentimentModel:
    settings = load_settings()

    return SentimentModel(
        SentimentModelConfig(
            model_path=settings.sentiment_model_path,
            model_version=settings.sentiment_model_version,
            batch_size=settings.sentiment_batch_size,
            max_length=settings.sentiment_max_length,
            neutral_floor=settings.sentiment_neutral_floor,
            device=settings.sentiment_device,
        )
    )


def crawl_new_posts_for_board(
        *,
        board: BoardConfig,
        http: HttpClient,
        store: CrawlStateStore,
        max_list_pages: int,
        max_posts_per_board: int,
) -> list[BoardPost]:
    crawler = InvenCrawler(
        board_id=board.board_id,
        board_base_url=board.board_base_url,
        http=http,
    )

    refs: list[BoardPostRef] = crawler.fetch_post_refs(
        max_pages=max_list_pages,
        max_posts=max_posts_per_board,
    )

    new_refs = store.filter_new_refs(refs)

    logger.info(
        "Board refs: board_id=%s board_name=%s total_refs=%s new_refs=%s",
        board.board_id,
        board.board_name,
        len(refs),
        len(new_refs),
    )

    return crawler.fetch_posts(new_refs)


def group_by_board(
        analyzed_posts: list[AnalyzedPost],
) -> dict[int, list[AnalyzedPost]]:
    grouped: dict[int, list[AnalyzedPost]] = {}

    for item in analyzed_posts:
        grouped.setdefault(item.board_id, []).append(item)

    return grouped


def get_base_hour() -> str:
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0).isoformat()


def main() -> None:
    settings = load_settings()

    boards_path = os.getenv("INVEN_BOARDS_CONFIG", "boards.json")
    state_db_path = os.getenv(
        "INVEN_STATE_DB_PATH",
        "./data/inven_sentiment_state.db",
    )

    boards = load_board_configs(boards_path)
    http = build_http_client()
    store = CrawlStateStore(state_db_path)

    try:
        all_new_posts: list[BoardPost] = []

        for board in boards:
            try:
                posts = crawl_new_posts_for_board(
                    board=board,
                    http=http,
                    store=store,
                    max_list_pages=settings.max_list_pages,
                    max_posts_per_board=settings.max_posts_per_run,
                )
                all_new_posts.extend(posts)

            except Exception as exc:
                logger.exception(
                    "Board crawl failed: board_id=%s board_name=%s err=%s",
                    board.board_id,
                    board.board_name,
                    exc,
                )

        logger.info("Total new posts fetched: %s", len(all_new_posts))

        if not all_new_posts:
            print(
                json.dumps(
                    {
                        "new_posts": 0,
                        "analyzed_posts": 0,
                    },
                    ensure_ascii=False,
                )
            )
            return

        model = build_sentiment_model()

        analyzed = analyze_posts(
            all_new_posts,
            model=model,
            text_used=settings.sentiment_text_used,  # type: ignore[arg-type]
        )

        logger.info("Total analyzed posts: %s", len(analyzed))

        saved_count = store.save_analyzed_posts(analyzed)
        logger.info("Saved analyzed posts: %s", saved_count)

        board_name_by_id = {
            board.board_id: board.board_name
            for board in boards
        }

        base_hour = get_base_hour()

        for board_id, items in group_by_board(analyzed).items():
            store.upsert_hourly_aggregate(
                base_hour=base_hour,
                board_id=board_id,
                board_name=board_name_by_id.get(board_id, str(board_id)),
                analyzed_posts=items,
            )

        result: dict[str, Any] = {
            "base_hour": base_hour,
            "boards": len(boards),
            "new_posts": len(all_new_posts),
            "analyzed_posts": len(analyzed),
            "saved_posts": saved_count,
        }

        print(json.dumps(result, ensure_ascii=False, indent=2))

    finally:
        store.close()


if __name__ == "__main__":
    main()