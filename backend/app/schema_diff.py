# backend/app/schema_diff.py
from dataclasses import dataclass
from typing import Dict, Any
import os

# Thresholds (tune via env vars or edit here)
ADDED_MAJOR_PCT = float(os.getenv("ADDED_MAJOR_PCT", "0.10"))        # added field >=10% of sample -> major
REMOVED_MAJOR_PREV_PCT = float(os.getenv("REMOVED_MAJOR_PREV_PCT", "0.20"))  # previously present >=20% -> removal major
REMOVED_MAJOR_NOW_PCT = float(os.getenv("REMOVED_MAJOR_NOW_PCT", "0.50"))    # now missing in >=50% -> major
TYPE_SHIFT_MAJOR_PCT = float(os.getenv("TYPE_SHIFT_MAJOR_PCT", "0.50"))      # new dominant type >=50%

@dataclass
class Diff:
    added: Dict[str, Any]
    removed: Dict[str, Any]
    changed: Dict[str, Any]

class Decision:
    def __init__(self, create_new_version: bool, reasons=None):
        self.create_new_version = create_new_version
        self.reasons = reasons or []

class SchemaDriftDetector:
    @staticmethod
    def compute_diff(old_schema: Dict[str, Any], new_schema: Dict[str, Any], field_stats: Dict[str,Any], latest_meta=None):
        """
        Return Diff with metadata: for added fields include present count; for changed include new_dom_pct; removed include prev_presence.
        """
        if not old_schema:
            # everything added
            props = (new_schema.get("properties") or {})
            added = {k: {"present": field_stats.get(k,{}).get("present",0)} for k in props}
            return Diff(added=added, removed={}, changed={})

        old_props = old_schema.get("properties", {}) or {}
        new_props = new_schema.get("properties", {}) or {}

        added = {}
        removed = {}
        changed = {}

        # added & changed
        for k, v in new_props.items():
            if k not in old_props:
                added[k] = {"present": field_stats.get(k,{}).get("present",0), "present_pct": field_stats.get(k,{}).get("present_pct",0)}
            else:
                if old_props[k] != v:
                    # determine new dominant type pct if available
                    tc = field_stats.get(k,{}).get("type_counts",{})
                    total = sum(tc.values()) if tc else 0
                    new_dom_pct = None
                    if total > 0:
                        new_type = max(tc, key=lambda x: tc[x])
                        new_dom_pct = tc[new_type] / total
                    changed[k] = {"old": old_props[k], "new": v, "new_dom_pct": new_dom_pct}

        # removed
        for k, v in old_props.items():
            if k not in new_props:
                prev_pct = None
                if latest_meta:
                    prev_pct = latest_meta.get("field_stats", {}).get(k, {}).get("present_pct")
                removed[k] = {"prev_presence_pct": prev_pct}

        return Diff(added=added, removed=removed, changed=changed)

    @staticmethod
    def decide(diff: Diff, sample_size: int, latest_meta=None):
        """
        Decide whether to create a new version (major drift) or not.
        Returns Decision(create_new_version: bool, reasons: list)
        """
        reasons = []

        # 1) Removed fields: if field was historically common and now missing -> major
        for f, info in (diff.removed or {}).items():
            prev_pct = info.get("prev_presence_pct", None)
            if prev_pct is not None and prev_pct >= REMOVED_MAJOR_PREV_PCT:
                # if it is removed now (by definition it's missing in new schema), consider major
                reasons.append(f"removed_common_field:{f}")
                return Decision(True, reasons)

        # 2) Added fields: if new field appears in >= ADDED_MAJOR_PCT of sampled docs -> major
        for f, info in (diff.added or {}).items():
            present = info.get("present", 0)
            present_pct = info.get("present_pct", 0)
            # if present count vs sample
            if sample_size > 0:
                if present_pct >= ADDED_MAJOR_PCT or present >= max(1, int(ADDED_MAJOR_PCT * sample_size)):
                    reasons.append(f"added_common_field:{f}")
                    return Decision(True, reasons)

        # 3) Type changes: if dominated by a new type with pct >= TYPE_SHIFT_MAJOR_PCT -> major
        for f, info in (diff.changed or {}).items():
            new_dom_pct = info.get("new_dom_pct", None)
            if new_dom_pct is not None and new_dom_pct >= TYPE_SHIFT_MAJOR_PCT:
                reasons.append(f"type_shift:{f}")
                return Decision(True, reasons)

        # 4) Otherwise zero or minor drift
        return Decision(False, reasons)
