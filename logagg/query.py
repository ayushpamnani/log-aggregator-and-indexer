"""Tiny query language.

    timeout gateway            -> both words (implicit AND)
    "connection refused"       -> exact phrase
    time*                      -> prefix match
    -healthcheck               -> exclude documents containing the word
    level:error service:auth   -> field filters (fields: service, level, host)
    a OR b                     -> union of two AND-groups
"""
from __future__ import annotations

import re
from dataclasses import dataclass

FIELDS = {"service", "level", "host"}
_TOKEN_RE = re.compile(r'-?"[^"]+"|\S+')


@dataclass
class Clause:
    value: str
    field: str | None = None
    negate: bool = False
    phrase: bool = False
    prefix: bool = False


def parse_query(q: str) -> list[list[Clause]]:
    """Returns a list of OR-groups; each group is a list of AND-ed clauses."""
    groups: list[list[Clause]] = [[]]
    for tok in _TOKEN_RE.findall(q or ""):
        if tok == "OR":
            if groups[-1]:
                groups.append([])
            continue
        negate = tok.startswith("-") and len(tok) > 1
        if negate:
            tok = tok[1:]
        if tok.startswith('"') and tok.endswith('"') and len(tok) > 2:
            groups[-1].append(Clause(tok[1:-1], negate=negate, phrase=True))
            continue
        field, sep, val = tok.partition(":")
        if sep and field.lower() in FIELDS and val:
            val = val.upper() if field.lower() == "level" else val.lower()
            groups[-1].append(Clause(val, field=field.lower(), negate=negate))
            continue
        if tok.endswith("*") and len(tok) > 1:
            groups[-1].append(Clause(tok[:-1].lower(), negate=negate, prefix=True))
        else:
            groups[-1].append(Clause(tok, negate=negate))
    return groups if groups[-1] or len(groups) > 1 else [[]]
