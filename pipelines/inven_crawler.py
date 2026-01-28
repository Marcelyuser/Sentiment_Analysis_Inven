from __future__ import annotations

import logging
import re
from typing import Iterable, Optional
from urllib.parse import urljoin, urlencode, urlparse, urlunparse, parse_qsl

from bs4 import BeautifulSoup

from pipelines.http_client import HttpClient
from pipelines.models import BoardPost, BoardPostRef

logger = logging.getLogger(__name__)


class InvenCrawler:
    """
    Crawler for m.inven.co.kr board pages.

    Scope:
    - Board list page: https://m.inven.co.kr/board/<game>/<board_id>
    - Post detail page: https://m.inven.co.kr/board/<game>/<board_id>/<post_id>

    Safety:
    - Does NOT access disallowed endpoints like /board/prevnext.php (robots disallow).
    - Uses conservative rate limit and retry via HttpClient.
    """

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

        # IMPORTANT: allow querystring, fragments, and absolute/relative URLs.
        # We'll match on parsed path only.
        # Example path: /board/lostark/5558/85872
        self._post_path_re = re.compile(rf"^/board/[^/]+/{self.board_id}/(\d+)$")

        self._disallowed_res = [re.compile(p) for p in self.DISALLOWED_PATH_PATTERNS]

    def fetch_board_list_html(self, page: int = 1) -> str:
        """Fetch raw board list HTML (useful for debugging DOM changes)."""
        list_url = self._build_list_url(page)
        logger.info("Fetching board list: url=%s", list_url)
        return self.http.get_text(list_url)

    def fetch_post_refs(self, max_pages: int = 1, max_posts: int = 30) -> list[BoardPostRef]:
        """
        Fetch post references from board list pages.

        Pagination:
        - Uses query parameter ?p=<page> for page >= 2 (common pattern).
        - If the site uses a different pagination scheme, max_pages=1 still works reliably.

        Raises:
            requests.RequestException / HTTPError: on persistent HTTP failures.
        """
        collected: dict[int, BoardPostRef] = {}

        for page in range(1, max_pages + 1):
            html = self.fetch_board_list_html(page)
            refs = self._parse_list_html(html)

            for ref in refs:
                collected[ref.post_id] = ref
                if len(collected) >= max_posts:
                    break

            if len(collected) >= max_posts:
                break

            if not refs:
                logger.info("No post refs found on page=%s; stopping pagination.", page)
                break

        return list(collected.values())

    def fetch_post(self, ref: BoardPostRef) -> BoardPost:
        """
        Fetch and parse a post detail page.

        Raises:
            ValueError: if URL path is disallowed by our safety rules
        """
        self._assert_allowed_url(ref.url)

        logger.info("Fetching post: post_id=%s url=%s", ref.post_id, ref.url)
        html = self.http.get_text(ref.url)
        return self._parse_post_html(html, ref)

    def fetch_posts(self, refs: Iterable[BoardPostRef]) -> list[BoardPost]:
        """
        Fetch multiple posts sequentially (conservative by design).
        Skips individual posts that fail parsing, but does not hide errors silently.
        """
        posts: list[BoardPost] = []
        for ref in refs:
            try:
                posts.append(self.fetch_post(ref))
            except Exception as e:
                logger.warning("Skipping post due to error: post_id=%s err=%s", ref.post_id, e)
        return posts

    # -------------------------
    # Parsing (unit-test target)
    # -------------------------

    def _parse_list_html(self, html: str) -> list[BoardPostRef]:
        soup = BeautifulSoup(html, "lxml")
        out: list[BoardPostRef] = []

        for a in soup.find_all("a", href=True):
            href_raw = a["href"].strip()

            # Normalize: relative or absolute -> absolute, then parse path only
            abs_url = urljoin("https://m.inven.co.kr", href_raw)
            parsed = urlparse(abs_url)
            path = parsed.path

            m = self._post_path_re.match(path)
            if not m:
                continue

            if self._is_disallowed_path(path):
                continue

            post_id = int(m.group(1))

            # Title: sometimes is in child nodes; get_text() covers nested spans
            title_raw = a.get_text(" ", strip=True)
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

        h2 = soup.find("h2")
        title = h2.get_text(" ", strip=True) if h2 else ref.title

        text_all = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text_all.splitlines() if ln.strip()]

        created_at = self._extract_created_at(lines)
        author = self._extract_author(lines)
        content = self._extract_content(lines, created_at=created_at, title=title)

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

    # -------------------------
    # Helpers
    # -------------------------

    def _build_list_url(self, page: int) -> str:
        if page <= 1:
            return self.board_base_url

        parsed = urlparse(self.board_base_url)
        q = dict(parse_qsl(parsed.query))
        q["p"] = str(page)
        new_query = urlencode(q)
        return urlunparse(parsed._replace(query=new_query))

    def _split_category(self, title_raw: str) -> tuple[Optional[str], str]:
        m = re.match(r"^\[([^\]]{1,6})\]\s*(.+)$", title_raw)
        if m:
            return m.group(1), m.group(2).strip()

        parts = title_raw.split(" ", 1)
        if len(parts) == 2 and 1 <= len(parts[0]) <= 6:
            cat = parts[0].strip()
            rest = parts[1].strip()
            if len(rest) >= 2:
                return cat, rest

        return None, title_raw.strip()

    def _extract_created_at(self, lines: list[str]) -> Optional[str]:
        for ln in lines[:140]:
            m = re.search(r"\b20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b", ln)
            if m:
                return m.group(0)
        return None

    def _extract_author(self, lines: list[str]) -> Optional[str]:
        for i, ln in enumerate(lines[:140]):
            if ln.startswith("조회:") and i > 0:
                cand = lines[i - 1]
                if 1 <= len(cand) <= 20 and all(x not in cand for x in ("게시판", "인벤", "광고")):
                    return cand
                return None
        return None

    def _extract_content(self, lines: list[str], created_at: Optional[str], title: str) -> str:
        stop_markers = {
            "추천확인",
            "신고",
            "스팸신고",
            "공유",
            "스크랩",
            "댓글쓰기",
            "댓글보기",
            "목록",
        }

        start_idx = 0
        if created_at and created_at in lines:
            start_idx = lines.index(created_at) + 1
        else:
            for i, ln in enumerate(lines[:90]):
                if title and title in ln:
                    start_idx = i + 1
                    break

        body: list[str] = []
        for ln in lines[start_idx:]:
            if ln in stop_markers:
                break
            if ln.startswith("지금 뜨는 인벤"):
                break
            if ln.startswith("조회:") or ln.startswith("추천:"):
                continue
            body.append(ln)

        return "\n".join(body).strip()

    def _is_disallowed_path(self, path: str) -> bool:
        return any(r.match(path) for r in self._disallowed_res)

    def _assert_allowed_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.netloc not in ("m.inven.co.kr", "www.inven.co.kr", "inven.co.kr"):
            raise ValueError(f"Unexpected host: {parsed.netloc}")
        if self._is_disallowed_path(parsed.path):
            raise ValueError(f"Disallowed path by safety rules: {parsed.path}")
