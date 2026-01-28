from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Sequence

from pipelines.models import BoardPost
from pipelines.sentiment_model import SentimentModel
from pipelines.sentiment_types import SentimentResult

logger = logging.getLogger(__name__)

TextUsed = Literal["title", "title+content"]


@dataclass(frozen=True)
class AnalyzedPost:
    """
    Output schema for downstream indexing (ES) later.

    Matches your project's base schema as closely as possible for step2.
    """
    board_id: int
    post_id: int
    url: str
    title: str
    content: str | None
    author: str | None
    created_at: str | None
    crawled_at: str  # ISO8601 UTC

    sentiment_label: str  # neg|neu|pos
    sentiment_score: float
    sentiment_probs: dict[str, float]
    model_version: str
    text_used: str  # title|title+content


def build_text(post: BoardPost, text_used: TextUsed) -> str:
    """
    Select text used for inference.

    Rules:
    - title: always use title
    - title+content: title + "\n\n" + content (if content exists)
    """
    title = (post.title or "").strip()
    if text_used == "title":
        return title

    content = (post.content or "").strip()
    if not content:
        return title
    if not title:
        return content
    return f"{title}\n\n{content}"


def analyze_posts(
        posts: Sequence[BoardPost],
        model: SentimentModel,
        text_used: TextUsed,
) -> list[AnalyzedPost]:
    """
    Analyze a batch of posts.

    - Does not mutate input posts
    - Keeps ordering
    """
    texts = [build_text(p, text_used) for p in posts]
    results: list[SentimentResult] = model.predict(texts)

    if len(results) != len(posts):
        raise RuntimeError("Sentiment results size mismatch")

    crawled_at = datetime.now(timezone.utc).isoformat()

    out: list[AnalyzedPost] = []
    for post, res in zip(posts, results):
        out.append(
            AnalyzedPost(
                board_id=post.board_id,
                post_id=post.post_id,
                url=post.url,
                title=post.title,
                content=post.content if post.content else None,
                author=post.author,
                created_at=post.created_at,
                crawled_at=crawled_at,
                sentiment_label=res.label,
                sentiment_score=res.score,
                sentiment_probs=dict(res.probs),
                model_version=model.model_version,
                text_used=text_used,
            )
        )
    return out
