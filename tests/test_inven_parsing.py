from __future__ import annotations

from pipelines.inven_crawler import InvenCrawler
from pipelines.http_client import HttpClient, HttpConfig
from pipelines.models import BoardPostRef


class _DummyHttp(HttpClient):
    def __init__(self):
        super().__init__(
            HttpConfig(
                timeout_sec=1.0,
                delay_sec=0.0,
                max_retries=0,
                backoff_base_sec=0.0,
                backoff_max_sec=0.0,
                user_agent="test",
            )
        )


def test_parse_list_html_extracts_refs_with_query_and_absolute_url():
    crawler = InvenCrawler(5558, "https://m.inven.co.kr/board/lostark/5558", _DummyHttp())
    html = """
    <html><body>
      <a href="/board/lostark/5558/85872?category=abc">잡담 테스트</a>
      <a href="https://m.inven.co.kr/board/lostark/5558/85873?p=1">[질문] 절대URL</a>
      <a href="/board/prevnext.php?idx=1">prevnext</a>
    </body></html>
    """
    refs = crawler._parse_list_html(html)
    ids = sorted([r.post_id for r in refs])
    assert ids == [85872, 85873]


def test_parse_post_html_extracts_created_at_and_content():
    crawler = InvenCrawler(5558, "https://m.inven.co.kr/board/lostark/5558", _DummyHttp())
    ref = BoardPostRef(
        board_id=5558,
        post_id=85872,
        url="https://m.inven.co.kr/board/lostark/5558/85872",
        title="dummy",
        category="잡담",
    )
    html = """
    <html><body>
      <h2>[잡담] 테스트 제목</h2>
      <div>닉네임123</div>
      <div>조회: 45</div>
      <div>2026-01-28 13:28:48</div>
      <div>첫 번째 줄 본문</div>
      <div>두 번째 줄 본문</div>
      <div>추천확인</div>
    </body></html>
    """
    post = crawler._parse_post_html(html, ref)
    assert post.title == "[잡담] 테스트 제목"
    assert post.created_at == "2026-01-28 13:28:48"
    assert "첫 번째 줄 본문" in post.content
    assert "두 번째 줄 본문" in post.content
