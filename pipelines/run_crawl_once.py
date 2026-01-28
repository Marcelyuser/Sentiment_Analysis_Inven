from __future__ import annotations

import json
import logging
from pathlib import Path

from pipelines.http_client import HttpClient, HttpConfig
from pipelines.inven_crawler import InvenCrawler
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

    # Fetch page-1 HTML once for optional dump when parsing yields 0 refs
    html_page1 = crawler.fetch_board_list_html(page=1)
    refs = crawler._parse_list_html(html_page1)

    if not refs and s.dump_html_on_empty:
        Path(s.dump_html_path).write_text(html_page1, encoding="utf-8")
        logger.warning("No refs parsed. Dumped HTML to: %s", s.dump_html_path)

    # Continue pagination only if we have refs on page1; otherwise stop (DOM likely changed)
    if refs:
        more = crawler.fetch_post_refs(max_pages=s.max_list_pages, max_posts=s.max_posts_per_run)
        refs = more

    logger.info("Fetched refs: %s", len(refs))

    posts = crawler.fetch_posts(refs)
    logger.info("Fetched posts: %s", len(posts))

    sample = [
        {
            "board_id": p.board_id,
            "post_id": p.post_id,
            "url": p.url,
            "category": p.category,
            "title": p.title,
            "author": p.author,
            "created_at": p.created_at,
            "content_preview": (p.content[:120] + "…") if len(p.content) > 120 else p.content,
        }
        for p in posts[:5]
    ]
    print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
