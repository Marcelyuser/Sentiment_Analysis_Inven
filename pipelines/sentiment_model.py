from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Sequence

import numpy as np
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from pipelines.sentiment_types import SentimentResult, SentimentLabel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SentimentModelConfig:
    model_path: str
    model_version: str
    batch_size: int
    max_length: int
    neutral_floor: float
    device: str  # "auto" | "cpu" | "cuda"


def _select_device(device: str) -> torch.device:
    if device == "cpu":
        return torch.device("cpu")
    if device == "cuda":
        return torch.device("cuda")
    # auto
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


@lru_cache(maxsize=1)
def _load_model_and_tokenizer(model_path: str):
    """
    Load once per process. Cached by model_path.

    Raises:
        OSError: if model files are missing or path is invalid.
    """
    logger.info("Loading sentiment model: path=%s", model_path)
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    return model, tokenizer


class SentimentModel:
    """
    Production-friendly wrapper:
    - model/tokenizer load once (process cache)
    - batch inference
    - consistent label mapping: neg/neu/pos

    Assumption:
      The fine-tuned model has 3 labels ordered as [neg, neu, pos].
      If your training used different ordering, we can make this configurable.
    """

    def __init__(self, cfg: SentimentModelConfig):
        self._cfg = cfg
        self._device = _select_device(cfg.device)

        model, tokenizer = _load_model_and_tokenizer(cfg.model_path)
        self._model = model.to(self._device)
        self._model.eval()
        self._tokenizer = tokenizer

        logger.info(
            "Sentiment model ready: version=%s device=%s batch=%s max_length=%s",
            cfg.model_version,
            self._device.type,
            cfg.batch_size,
            cfg.max_length,
        )

    @property
    def model_version(self) -> str:
        return self._cfg.model_version

    def predict(self, texts: Sequence[str]) -> list[SentimentResult]:
        """
        Predict sentiment for a list of texts.

        Rules:
        - empty/blank -> neutral with probs (0,1,0)
        - returns results in same order as input

        Raises:
            ValueError: if batch_size/max_length invalid
        """
        if self._cfg.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self._cfg.max_length <= 0:
            raise ValueError("max_length must be > 0")

        results: list[SentimentResult] = []
        for batch in _batched(texts, self._cfg.batch_size):
            results.extend(self._predict_batch(batch))
        return results

    def _predict_batch(self, texts: Sequence[str]) -> list[SentimentResult]:
        prepared: list[str] = []
        empty_mask: list[bool] = []
        for t in texts:
            if t is None or not str(t).strip():
                prepared.append("")  # placeholder
                empty_mask.append(True)
            else:
                prepared.append(str(t))
                empty_mask.append(False)

        # Tokenize
        enc = self._tokenizer(
            prepared,
            padding=True,
            truncation=True,
            max_length=self._cfg.max_length,
            return_tensors="pt",
        )
        enc = {k: v.to(self._device) for k, v in enc.items()}

        with torch.no_grad():
            logits = self._model(**enc).logits  # (B, 3)
            probs = torch.softmax(logits, dim=-1).cpu().numpy()

        out: list[SentimentResult] = []
        for i, p in enumerate(probs):
            if empty_mask[i]:
                out.append(
                    SentimentResult(
                        label="neu",
                        score=0.0,
                        probs={"neg": 0.0, "neu": 1.0, "pos": 0.0},
                    )
                )
                continue

            # label order assumption: [neg, neu, pos]
            neg, neu, pos = float(p[0]), float(p[1]), float(p[2])
            max_prob = max(neg, neu, pos)

            if self._cfg.neutral_floor > 0.0 and max_prob < self._cfg.neutral_floor:
                label: SentimentLabel = "neu"
            else:
                label = _argmax_label(neg, neu, pos)

            score = pos - neg
            out.append(
                SentimentResult(
                    label=label,
                    score=float(score),
                    probs={"neg": neg, "neu": neu, "pos": pos},
                )
            )
        return out


def _argmax_label(neg: float, neu: float, pos: float) -> SentimentLabel:
    if pos >= neu and pos >= neg:
        return "pos"
    if neg >= neu and neg >= pos:
        return "neg"
    return "neu"


def _batched(items: Sequence[str], batch_size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]
