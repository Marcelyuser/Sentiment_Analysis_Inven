from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping


SentimentLabel = Literal["neg", "neu", "pos"]


@dataclass(frozen=True)
class SentimentResult:
    """
    Standardized sentiment output.

    - label: neg|neu|pos
    - score: pos_prob - neg_prob (range ~[-1, 1])
    - probs: mapping of neg/neu/pos to probability (sum ~ 1)
    """

    label: SentimentLabel
    score: float
    probs: Mapping[SentimentLabel, float]
