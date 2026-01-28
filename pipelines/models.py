from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BoardPostRef:
    """Lightweight reference parsed from a board list page."""

    board_id: int
    post_id: int
    url: str
    title: str
    category: Optional[str] = None


@dataclass(frozen=True)
class BoardPost:
    """Full post object parsed from a post detail page."""

    board_id: int
    post_id: int
    url: str
    title: str
    category: Optional[str]
    author: Optional[str]
    created_at: Optional[str]
    content: str
