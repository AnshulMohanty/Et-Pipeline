# backend/app/schema_infer.py
from genson import SchemaBuilder
from collections import defaultdict, Counter

def _detect_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "unknown"

def infer_schema_from_sample(docs):
    """
    Build a JSON Schema using GenSON and also return per-field stats:
    returns (schema_dict, field_stats)
    field_stats[field] = {"present": int, "present_pct": float, "type_counts": {type:count}}
    """
    builder = SchemaBuilder()
    presence = defaultdict(int)
    type_counts = defaultdict(Counter)
    sample_size = len(docs)
    for d in docs:
        builder.add_object(d)
        for k, v in d.items():
            presence[k] += 1
            t = _detect_type(v)
            type_counts[k][t] += 1

    field_stats = {}
    for k in presence:
        total = sample_size if sample_size else 1
        field_stats[k] = {
            "present": presence[k],
            "present_pct": presence[k] / total,
            "type_counts": dict(type_counts[k])
        }

    schema = builder.to_schema()
    # remove $schema if present and order properties
    if "$schema" in schema:
        schema.pop("$schema", None)
    if "properties" in schema:
        props = schema["properties"]
        schema["properties"] = {pk: props[pk] for pk in sorted(props.keys())}
    return schema, field_stats
