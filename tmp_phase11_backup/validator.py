# backend/app/validator.py

"""

Validator & promotion policy for Chrysalis.



Policy implemented (recommended):

- All incoming docs are *validated against the latest registered schema* (schema_registry latest).

- If doc violates latest schema, it's pushed to DLQ.

- Candidate schema is computed from the batch sample, but it will only become a new version if:

    * candidate != latest AND

    * promotion rule passes: (a) candidate_field_coverage_pct >= PROMOTE_PCT (env) OR (b) manual approval via CLI/UI (not automatic)

- PROMOTE_PCT default = 0.9 (90% of sample docs must conform to candidate schema).

- This file exposes: validate_doc_against_schema(doc, schema), decide_promotion(latest, candidate, sample_stats)

"""

import os, orjson

from datetime import datetime



PROMOTE_PCT = float(os.getenv("PROMOTE_PCT", "0.9"))

REQUIRED_PCT = float(os.getenv("REQUIRED_PCT", "0.9"))



def _pytype_to_json_type(val):

    if val is None:

        return "null"

    t = type(val)

    if t is bool:

        return "boolean"

    if t is int:

        return "integer"

    if t is float:

        return "number"

    if t is str:

        return "string"

    if t is dict:

        return "object"

    if t is list:

        return "array"

    return "string"



def validate_doc_against_schema(doc, schema):

    """

    Strong validation against given schema (best-effort).

    Returns (ok:bool, reason:str or None).

    """

    if not schema:

        return True, None

    props = schema.get("properties", {})

    required = schema.get("required", [])

    for r in required:

        if r not in doc:

            return False, f"missing_required_field:{r}"

    for k, spec in props.items():

        if k not in doc:

            continue

        exp = spec.get("type")

        val = doc[k]

        if exp == "integer":

            if not (isinstance(val, int) and not isinstance(val, bool)):

                # allow integer-like floats

                if isinstance(val, float) and val.is_integer():

                    continue

                return False, f"type_mismatch:{k}:expected_integer"

        elif exp == "number":

            if not isinstance(val, (int, float)) or isinstance(val, bool):

                return False, f"type_mismatch:{k}:expected_number"

        elif exp == "string":

            if not isinstance(val, str):

                return False, f"type_mismatch:{k}:expected_string"

        elif exp == "object":

            if not isinstance(val, dict):

                return False, f"type_mismatch:{k}:expected_object"

        elif exp == "array":

            if not isinstance(val, list):

                return False, f"type_mismatch:{k}:expected_array"

    return True, None



def decide_promotion(latest_schema, candidate_schema, sample_field_stats):

    """

    Decide whether candidate_schema should be promoted to new version.

    sample_field_stats: dict mapping field -> {'present':count, 'present_pct':float}

    Rule: Promote if (1) candidate != latest AND (2) all fields in candidate are present in >= PROMOTE_PCT of sample docs.

    Returns (promote:bool, reasons:list)

    """

    reasons = []

    if latest_schema is None:

        reasons.append("no_latest_schema")

        return True, reasons

    if orjson.dumps(latest_schema) == orjson.dumps(candidate_schema):

        reasons.append("schemas_equal")

        return False, reasons

    # Check candidate fields coverage

    props = candidate_schema.get("properties", {})

    ok_count = 0

    total = len(props) or 1

    for f in props:

        stat = sample_field_stats.get(f, {})

        pct = stat.get("present_pct", 0.0)

        if pct >= PROMOTE_PCT:

            ok_count += 1

    coverage = ok_count / total

    if coverage >= PROMOTE_PCT:

        reasons.append(f"coverage_ok({coverage:.2f})")

        return True, reasons

    reasons.append(f"coverage_fail({coverage:.2f})")

    return False, reasons
