from typing import List, Optional
from .models import EODPrice, ReconciledRecord
from statistics import mean
from collections import Counter

class ReconciliationEngine:
    """
    Reconciles data from multiple vendors to produce high quality, auditable records.
    Currently supports EODPrice. Extend for other types as needed.
    """
    def __init__(self, vendor_priority: Optional[List[str]] = None):
        self.vendor_priority = vendor_priority or []

    def reconcile_eod_prices(self, records: List[EODPrice]) -> Optional[ReconciledRecord]:
        if not records:
            return None
        # Group by field
        close_prices = [r.close for r in records if r.close is not None]
        open_prices = [r.open for r in records if r.open is not None]
        high_prices = [r.high for r in records if r.high is not None]
        low_prices = [r.low for r in records if r.low is not None]
        volume_vals = [r.volume for r in records if r.volume is not None]
        vendors = [r.vendor for r in records if r.vendor]
        # Consensus/majority for close price
        close_counter = Counter(close_prices)
        close, close_votes = close_counter.most_common(1)[0] if close_counter else (None, 0)
        # Vendor priority fallback
        chosen_vendor = None
        rationale = ""
        audit_log = []
        if close_votes == 1 and self.vendor_priority:
            for v in self.vendor_priority:
                for r in records:
                    if r.vendor == v and r.close is not None:
                        close = r.close
                        chosen_vendor = v
                        rationale = f"Used {v} due to vendor priority."
                        audit_log.append({
                            "decision": "vendor_priority",
                            "vendor": v,
                            "record": r.model_dump(),
                        })
                        break
                if chosen_vendor:
                    break
        else:
            rationale = "Used consensus/majority value."
            audit_log.append({
                "decision": "consensus_majority",
                "close_candidates": close_prices,
                "close_value": close,
                "votes": close_votes,
            })
        # Aggregate other fields (mean)
        open_val = mean(open_prices) if open_prices else None
        high_val = mean(high_prices) if high_prices else None
        low_val = mean(low_prices) if low_prices else None
        volume_val = mean(volume_vals) if volume_vals else None
        sources = list(set(vendors))
        quality_score = min(1.0, close_votes / max(1, len(records)))
        provenance = {
            "raw_records": [r.model_dump() for r in records],
            "audit_log": audit_log,
            "scoring": {
                "close_votes": close_votes,
                "total_records": len(records),
                "quality_score": quality_score,
            }
        }
        return ReconciledRecord(
            data_type="eod",
            instrument_id=records[0].instrument_id,
            as_of=records[0].date,
            value={
                "close": close,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "volume": volume_val
            },
            quality_score=quality_score,
            sources=sources,
            rationale=rationale,
            provenance=provenance
        )
