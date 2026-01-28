from __future__ import annotations

from pipelines.models import BoardPost
from pipelines.sentiment_pipeline import build_text


def _post(title: str, content: str) -> BoardPost:
    return BoardPost(
        board_id=5558,
        post_id=1,
        url="https://m.inven.co.kr/board/lostark/5558/1",
        title=title,
        category=None,
        author=None,
        created_at=None,
        content=content,
    )


def test_build_text_title_only():
    p = _post("제목", "본문")
    assert build_text(p, "title") == "제목"


def test_build_text_title_plus_content():
    p = _post("제목", "본문")
    assert build_text(p, "title+content") == "제목\n\n본문"


def test_build_text_title_plus_content_when_content_empty():
    p = _post("제목", "")
    assert build_text(p, "title+content") == "제목"
