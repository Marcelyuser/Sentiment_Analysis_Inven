from __future__ import annotations

import json
import logging
from pathlib import Path

from pipelines.http_client import HttpClient, HttpConfig
from pipelines.inven_crawler import InvenCrawler
from pipelines.sentiment_model import SentimentModel, SentimentModelConfig
from pipelines.sentiment_pipeline import analyze_posts
from pipelines.settings import load_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    s = load_settings()

    http = HttpClient(
        HttpConfig(
            timeout_sec=s.request_timeout_sec,
            delay_sec=s.request_delay_sec,
            max_retries=s.max_retries,
            backoff_base_sec=s.backoff_base_sec,
            backoff_max_sec=s.backoff_max_sec,
            user_agent=s.user_agent,
        )
    )

    crawler = InvenCrawler(
        board_id=s.inven_board_id,
        board_base_url=s.inven_board_base_url,
        http=http,
    )

    # Same debug behavior as before: dump when empty
    html_page1 = crawler.fetch_board_list_html(page=1)
    refs = crawler._parse_list_html(html_page1)
    if not refs and s.dump_html_on_empty:
        Path(s.dump_html_path).write_text(html_page1, encoding="utf-8")
        logger.warning("No refs parsed. Dumped HTML to: %s", s.dump_html_path)

    if refs:
        refs = crawler.fetch_post_refs(max_pages=s.max_list_pages, max_posts=s.max_posts_per_run)

    logger.info("Fetched refs: %s", len(refs))
    posts = crawler.fetch_posts(refs)
    logger.info("Fetched posts: %s", len(posts))

    if not posts:
        print("[]")
        return

    model = SentimentModel(
        SentimentModelConfig(
            model_path=s.sentiment_model_path,
            model_version=s.sentiment_model_version,
            batch_size=s.sentiment_batch_size,
            max_length=s.sentiment_max_length,
            neutral_floor=s.sentiment_neutral_floor,
            device=s.sentiment_device,
        )
    )

    analyzed = analyze_posts(posts, model=model, text_used=s.sentiment_text_used)  # type: ignore[arg-type]
    logger.info("Analyzed posts: %s", len(analyzed))

    # Minimal output for inspection (CLI only)
    sample = [
        {
            "doc_id": f"{a.board_id}:{a.post_id}",
            "title": a.title,
            "sentiment_label": a.sentiment_label,
            "sentiment_score": a.sentiment_score,
            "sentiment_probs": a.sentiment_probs,
            "text_used": a.text_used,
            "model_version": a.model_version,
            "url": a.url,
        }
        for a in analyzed[:10]
    ]
    print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
