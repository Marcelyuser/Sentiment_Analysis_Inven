from __future__ import annotations

import json
import logging
from typing import Any

from pipelines.http_client import HttpClient, HttpConfig
from pipelines.inven_crawler import InvenCrawler
from pipelines.kafka_producer import InvenKafkaProducer, KafkaProducerConfig
from pipelines.sentiment_model import SentimentModel, SentimentModelConfig
from pipelines.sentiment_pipeline import analyze_posts
from pipelines.settings import load_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _to_kafka_payload(analyzed: Any) -> dict[str, Any]:
    """
    Convert analyzed item (your project's analyzed post object) to Kafka payload dict.

    Assumes analyzed has attributes:
      board_id, post_id, title, url,
      sentiment_label, sentiment_score, sentiment_probs,
      text_used, model_version
    """
    return {
        "doc_id": f"{analyzed.board_id}:{analyzed.post_id}",
        "board_id": analyzed.board_id,
        "post_id": analyzed.post_id,
        "title": analyzed.title,
        "url": analyzed.url,
        "sentiment_label": analyzed.sentiment_label,
        "sentiment_score": analyzed.sentiment_score,
        "sentiment_probs": analyzed.sentiment_probs,
        "text_used": analyzed.text_used,
        "model_version": analyzed.model_version,
    }


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

    payloads = [_to_kafka_payload(a) for a in analyzed]
    # (선택) 눈으로 확인할 최소 출력
    print(json.dumps(payloads[:3], ensure_ascii=False, indent=2))

    producer_cfg = KafkaProducerConfig.from_env()
    producer = InvenKafkaProducer(producer_cfg)
    try:
        sent = producer.send_many(payloads)
        logger.info("Produced messages: %s topic=%s", sent, producer_cfg.topic)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
