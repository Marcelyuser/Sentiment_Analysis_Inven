from __future__ import annotations

import logging
import re
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from pipelines.http_client import HttpClient
from pipelines.models import BoardPost, BoardPostRef

logger = logging.getLogger(__name__)


class InvenCrawler:
    """
    Inven mobile board crawler (supports multiple board layouts).
    """

    SIMPLE_BOARDS = {5861}   # 도화가 게시판
    CATEGORY_BOARDS = {5558} # 직업/카테고리 게시판

    DISALLOWED_PATH_PATTERNS = (
        r"^/board/prevnext\.php$",
        r"^/powerbbs/prevnext\.php$",
        r"^/staff/.*",
        r"^/webzine/prevnext\.php$",
    )

    def __init__(self, board_id: int, board_base_url: str, http: HttpClient):
        self.board_id = int(board_id)
        self.board_base_url = board_base_url.rstrip("/")
        self.http = http

        # post_id는 path에서만 추출
        self._post_id_re = re.compile(rf"/board/[^/]+/{self.board_id}/(\d+)")

        self._disallowed_res = [re.compile(p) for p in self.DISALLOWED_PATH_PATTERNS]

    # =========================
    # Public APIs
    # =========================

    def fetch_board_list_html(self, page: int = 1) -> str:
        logger.info("Fetching board list: url=%s", self.board_base_url)
        return self.http.get_text(self.board_base_url)

    def fetch_post_refs(self, max_pages: int = 1, max_posts: int = 30) -> list[BoardPostRef]:
        html = self.fetch_board_list_html()
        refs = self._parse_list_html(html)
        return refs[:max_posts]

    def fetch_post(self, ref: BoardPostRef) -> BoardPost:
        self._assert_allowed_url(ref.url)
        html = self.http.get_text(ref.url)
        return self._parse_post_html(html, ref)

    def fetch_posts(self, refs: Iterable[BoardPostRef]) -> list[BoardPost]:
        posts: list[BoardPost] = []
        for ref in refs:
            try:
                posts.append(self.fetch_post(ref))
            except Exception as e:
                logger.warning("Skipping post %s: %s", ref.post_id, e)
        return posts

    # =========================
    # Parsing
    # =========================

    def _parse_list_html(self, html: str) -> list[BoardPostRef]:
        soup = BeautifulSoup(html, "lxml")
        out: list[BoardPostRef] = []

        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            abs_url = urljoin("https://m.inven.co.kr", href)

            parsed = urlparse(abs_url)
            if self._is_disallowed_path(parsed.path):
                continue

            m = self._post_id_re.search(parsed.path)
            if not m:
                continue

            post_id = int(m.group(1))

            # ✅ 게시판별 제목 selector
            title_raw = self._extract_title_from_list(a)
            if not title_raw:
                continue

            category, title = self._split_category(title_raw)

            out.append(
                BoardPostRef(
                    board_id=self.board_id,
                    post_id=post_id,
                    url=abs_url,
                    title=title,
                    category=category,
                )
            )

        return out

    def _parse_post_html(self, html: str, ref: BoardPostRef) -> BoardPost:
        soup = BeautifulSoup(html, "lxml")

        # 상세 페이지 제목은 신뢰하지 않고 ref.title 우선
        title = ref.title

        text_all = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text_all.splitlines() if ln.strip()]

        created_at = self._extract_created_at(lines)
        author = self._extract_author(lines)
        content = self._extract_content(lines, created_at, title)

        return BoardPost(
            board_id=ref.board_id,
            post_id=ref.post_id,
            url=ref.url,
            title=title,
            category=ref.category,
            author=author,
            created_at=created_at,
            content=content,
        )

    # =========================
    # Helpers
    # =========================

    def _extract_title_from_list(self, a) -> Optional[str]:
        # 5861
        if self.board_id in self.SIMPLE_BOARDS:
            el = a.select_one("span.subject")
            if el:
                return el.get_text(strip=True)

        # 5558
        if self.board_id in self.CATEGORY_BOARDS:
            el = (
                    a.select_one("strong.subject")
                    or a.select_one("span.subject")
                    or a.select_one("div.subject")
            )
            if el:
                return el.get_text(strip=True)

        return None

    def _split_category(self, title_raw: str) -> tuple[Optional[str], str]:
        m = re.match(r"^\[([^\]]{1,6})\]\s*(.+)$", title_raw)
        if m:
            return m.group(1), m.group(2).strip()
        return None, title_raw.strip()

    def _extract_created_at(self, lines: list[str]) -> Optional[str]:
        for ln in lines[:120]:
            m = re.search(r"\b20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}\b", ln)
            if m:
                return m.group(0)
        return None

    def _extract_author(self, lines: list[str]) -> Optional[str]:
        for i, ln in enumerate(lines[:120]):
            if ln.startswith("조회:") and i > 0:
                cand = lines[i - 1]
                if 1 <= len(cand) <= 20:
                    return cand
        return None

    def _extract_content(self, lines: list[str], created_at: Optional[str], title: str) -> str:
        start_idx = 0
        if created_at and created_at in lines:
            start_idx = lines.index(created_at) + 1

        body = []
        for ln in lines[start_idx:]:
            if ln in {"댓글쓰기", "댓글보기", "목록"}:
                break
            if ln.startswith(("조회:", "추천:")):
                continue
            body.append(ln)

        return "\n".join(body).strip()

    def _is_disallowed_path(self, path: str) -> bool:
        return any(r.match(path) for r in self._disallowed_res)

    def _assert_allowed_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.netloc not in ("m.inven.co.kr", "www.inven.co.kr", "inven.co.kr"):
            raise ValueError(f"Unexpected host: {parsed.netloc}")
