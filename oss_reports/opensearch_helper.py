# opensearch_helper_v2.py
# Python 3.6 compatible
from __future__ import print_function

import time
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from opensearchpy import OpenSearch, helpers

# ---------------- Date detection (3.6-friendly) ----------------

ISO_CANDIDATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)

DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",           # 2025-09-02T12:34:56Z
    "%Y-%m-%d %H:%M:%S",            # 2025-09-02 12:34:56
    "%Y-%m-%dT%H:%M:%S",            # 2025-09-02T12:34:56
    "%Y-%m-%dT%H:%M:%S.%fZ",        # 2025-09-02T12:34:56.123Z
    "%Y-%m-%dT%H:%M:%S.%f",         # 2025-09-02T12:34:56.123
    "%Y-%m-%d %H:%M:%S.%f",         # 2025-09-02 12:34:56.123
    # Note: %z with offset strings is finicky on 3.6; we keep Z/naive forms.
]

def _looks_iso_datetime(s):  # type: (str) -> bool
    if not isinstance(s, str):
        return False
    if not ISO_CANDIDATE_RE.match(s.strip()):
        return False
    raw = s.strip()
    raw_z = raw.replace("+00:00", "Z")  # normalize
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(raw_z, fmt)
            return True
        except ValueError:
            continue
    return False

# ---------------- Safer epoch detection ----------------

def _epoch_unit(n):  # type: (float) -> Optional[str]
    """
    Return 'epoch_second' or 'epoch_millis' only for plausible *integral* epochs
    in range 1970-01-01 .. 2100-01-01. Reject floats and out-of-range big IDs.
    """
    try:
        x = float(n)
        if not x.is_integer():
            return None
        xi = int(x)
    except Exception:
        return None

    # digits guard: seconds ~10 digits, millis ~13 digits (today)
    d = len(str(abs(xi)))
    if d not in (10, 13):
        return None

    MAX_EPOCH_S = 4102444800   # 2100-01-01T00:00:00Z
    MIN_EPOCH_S = 0

    if MIN_EPOCH_S <= xi <= MAX_EPOCH_S:
        return "epoch_second"

    xi_ms = xi
    if MIN_EPOCH_S * 1000 <= xi_ms <= MAX_EPOCH_S * 1000:
        return "epoch_millis"

    return None

# Only time-ish field names are eligible for epoch detection
DATE_NAME_RE = re.compile(r"(?:^|_)(time|timestamp|ts|date)(?:$|_)", re.I)

# ---------------- Type inference helpers ----------------

def _merge_type(existing, new):  # type: (str, str) -> str
    """
    Priority: string > date > float > integer
    - string wins to avoid mapping conflicts
    - float wins over integer to accommodate mixed numeric forms
    """
    if existing == new:
        return existing
    pr = {"string": 4, "date": 3, "float": 2, "integer": 1}
    return existing if pr.get(existing, 0) >= pr.get(new, 0) else new

def _field_type_for_value(v, field_name=None):  # type: (Any, Optional[str]) -> Tuple[str, Optional[str]]
    """
    Returns (logical_type, epoch_hint)
      logical_type in {'string','date','float','integer'}
      epoch_hint in {'epoch_second','epoch_millis',None}
    """
    # strings
    if isinstance(v, str):
        if _looks_iso_datetime(v):
            return "date", None
        return "string", None

    # numbers
    if isinstance(v, bool):
        # bool is a subclass of int in Python; treat separately as string/keyword
        return "string", None

    if isinstance(v, int):
        # Only treat as epoch if field name looks like a timestamp
        if field_name and DATE_NAME_RE.search(field_name or ""):
            hint = _epoch_unit(v)
            if hint:
                return "date", hint
        return "integer", None

    if isinstance(v, float):
        if field_name and DATE_NAME_RE.search(field_name or ""):
            hint = _epoch_unit(v)
            if hint:
                return "date", hint
        return "float", None

    # everything else → string/keyword
    return "string", None

# ---------------- Mapping inference with overrides ----------------

def infer_mapping(docs, type_overrides=None):  # type: (List[Dict[str, Any]], Optional[Dict[str, Dict[str, Any]]]) -> Dict[str, Any]
    """
    Builds an index body (settings + mappings) from sample docs.
    type_overrides: dict[field] = {"type": "...", ...} (exact OpenSearch field mapping)
    """
    type_overrides = type_overrides or {}
    field_types = {}   # type: Dict[str, str]
    epoch_hints = {}   # type: Dict[str, set]

    for doc in docs or []:
        for k, v in doc.items():
            if k in type_overrides:
                # Skip inference for overridden fields
                continue
            t, hint = _field_type_for_value(v, field_name=k)
            if k not in field_types:
                field_types[k] = t
            else:
                field_types[k] = _merge_type(field_types[k], t)
            if t == "date" and hint:
                epoch_hints.setdefault(k, set()).add(hint)

    props = {}

    # apply inferred fields
    for field, ftype in field_types.items():
        if ftype == "string":
            props[field] = {"type": "keyword", "ignore_above": 1024}
        elif ftype == "integer":
            props[field] = {"type": "long"}
        elif ftype == "float":
            props[field] = {"type": "float"}
        elif ftype == "date":
            hints = epoch_hints.get(field, set())
            fmts = [
                "strict_date_optional_time",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            ]
            # include only the epoch formats actually seen (or none)
            if "epoch_second" in hints:
                fmts.append("epoch_second")
            if "epoch_millis" in hints:
                fmts.append("epoch_millis")
            props[field] = {"type": "date", "format": "||".join(fmts)}
        else:
            props[field] = {"type": "keyword", "ignore_above": 1024}

    # apply explicit overrides last (win over inference)
    for field, mapping in type_overrides.items():
        props[field] = mapping

    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {"dynamic": "strict", "properties": props},
    }

# ---------------------- Public Helper Class --------------------------

class OSWriter(object):
    """
    Reusable OpenSearch writer for pipelines, credential-free.
    You MUST pass an already-initialized OpenSearch client.

    Example:
        client = OpenSearch(...)  # build outside (env vars, vault, etc.)
        writer = OSWriter(client)
        writer.push(index_name="my-index", docs=list_of_dicts, id_field="id")
    """

    def __init__(self, client):  # type: (OpenSearch) -> None
        if client is None:
            raise ValueError("OpenSearch client is required")
        self.client = client

    def ensure_index(self,
                     index_name,        # type: str
                     sample_docs,       # type: List[Dict[str, Any]]
                     type_overrides=None  # type: Optional[Dict[str, Dict[str, Any]]]
                     ):                 # type: (...) -> None
        if self.client.indices.exists(index=index_name):
            return
        body = infer_mapping(sample_docs or [], type_overrides=type_overrides)
        self.client.indices.create(index=index_name, body=body)

    def bulk_push(self,
                  index_name,          # type: str
                  docs,                # type: List[Dict[str, Any]]
                  id_field=None,       # type: Optional[str]
                  chunk_size=2000,     # type: int
                  max_retries=2,       # type: int
                  backoff_sec=2.0,     # type: float
                  type_overrides=None  # type: Optional[Dict[str, Dict[str, Any]]]
                  ):                   # type: (...) -> None
        """
        Push docs with retries. If id_field is provided and present in doc,
        it's used as _id (upsert semantics).
        """
        if not isinstance(docs, list) or (docs and not isinstance(docs[0], dict)):
            raise ValueError("`docs` must be a list of dictionaries")

        self.ensure_index(index_name, docs, type_overrides=type_overrides)

        actions = []
        for d in docs:
            action = {"_index": index_name, "_source": d}
            if id_field and id_field in d:
                action["_id"] = d[id_field]
            actions.append(action)

        attempt = 0
        while True:
            try:
                helpers.bulk(
                    self.client,
                    actions,
                    chunk_size=chunk_size,
                    request_timeout=120,
                    raise_on_error=True,
                )
                return
            except Exception:
                if attempt >= max_retries:
                    raise
                attempt += 1
                time.sleep(backoff_sec)

    def push(self,
             index_name,              # type: str
             docs,                    # type: List[Dict[str, Any]]
             id_field=None,           # type: Optional[str]
             type_overrides=None      # type: Optional[Dict[str, Dict[str, Any]]]
             ):                       # type: (...) -> None
        self.bulk_push(
            index_name=index_name,
            docs=docs,
            id_field=id_field,
            type_overrides=type_overrides,
        )

