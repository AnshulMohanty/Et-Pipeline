# backend/app/validator.py
"""
Schema-driven validator.

Logic:
- Determine 'required' fields from field_stats in the latest schema meta.
  A field is considered required if its historical presence pct >= REQUIRED_PCT.
- Type checking uses a permissive compatibility set and allows a small set of promotions.
- If a required field is missing OR a type is incompatible (and cannot be promoted), the doc is invalid.
"""

from typing import Tuple, Dict, Any
import os

# Thresholds (tunable via env)
REQUIRED_PCT = float(os.getenv("REQUIRED_PCT", "0.9"))  # if historically present >= 90% -> required
ALLOW_TYPE_PROMOTION = os.getenv("ALLOW_TYPE_PROMOTION", "true").lower() in ("1","true","yes")

def _detect_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    # ints are also numbers, record both as 'integer' vs 'number' where needed
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "unknown"

def _is_type_compatible(value, schema_prop) -> bool:
    """
    Check if Python value is compatible with schema property definition.
    schema_prop is expected to be a JSON Schema fragment, e.g. {'type': 'string'} or {'type': ['string','null']}
    """
    val_type = _detect_type(value)
    wanted = schema_prop.get("type") if isinstance(schema_prop, dict) else None

    # normalize wanted to list
    wanted_types = []
    if isinstance(wanted, list):
        wanted_types = wanted
    elif isinstance(wanted, str):
        wanted_types = [wanted]
    elif wanted is None:
        # schema didn't specify type -> accept anything
        return True
    else:
        # unknown shape -> accept
        return True

    # direct match
    if val_type in wanted_types:
        return True

    # integer is compatible with number
    if val_type == "integer" and "number" in wanted_types:
        return True

    # allow integer/number if schema expects string only when promotion enabled (we might coerce) — be conservative: treat as incompatible
    # but we will allow number->string only if ALLOW_TYPE_PROMOTION and value can be losslessly represented as string (we allow that)
    if ALLOW_TYPE_PROMOTION:
        if val_type in ("integer","number") and "string" in wanted_types:
            return True  # consider ok (we can store as string if desired)
        if val_type == "string" and "number" in wanted_types:
            # string to number only if string is numeric
            try:
                float(value)
                return True
            except Exception:
                return False

    return False

def determine_required_fields(field_stats: Dict[str,Any]) -> Dict[str, float]:
    """
    Returns dict of fields that should be treated as required with their historical present_pct.
    """
    required = {}
    for f, info in (field_stats or {}).items():
        pct = info.get("present_pct", 0)
        if pct >= REQUIRED_PCT:
            required[f] = pct
    return required

def validate_doc_strict(doc: Dict[str,Any], schema: Dict[str,Any], field_stats: Dict[str,Any]) -> Tuple[bool, str]:
    """
    Returns (ok: bool, reason: str|None).
    - Checks required fields
    - Checks types using schema properties and field_stats as guidance
    """
    if not isinstance(doc, dict):
        return False, "not_an_object"

    props = (schema or {}).get("properties", {}) or {}
    required_fields = determine_required_fields(field_stats)

    # 1) required fields
    for req_field in required_fields:
        if req_field not in doc:
            return False, f"missing_required:{req_field}"

    # 2) type checks
    for f, val in doc.items():
        if f in props:
            prop_def = props[f]
            ok = _is_type_compatible(val, prop_def)
            if not ok:
                return False, f"type_mismatch:{f}"

    # otherwise okay
    return True, None
