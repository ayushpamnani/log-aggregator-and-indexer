"""Tokenizer shared by the indexer and the query parser (they MUST agree)."""
import re

_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())
