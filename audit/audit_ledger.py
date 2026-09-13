"""
RecoverSense — Tamper-Evident Audit Ledger
==============================================

Every decision the system makes is recorded as a chained record:

    hash_i = SHA256(hash_{i-1} + canonical_json(record_i))

This is explicitly called a "tamper-evident" ledger, NOT an "immutable"
one — a normal append-only JSON/DB log with a hash chain lets you DETECT
tampering (any edit breaks the chain from that point forward) but does
not, by itself, prevent someone with DB write access from rewriting the
whole chain. True immutability would require write-once storage / an
external anchor, which is out of scope for this prototype.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional

GENESIS_HASH = "0" * 64
# Records written before channel economics was added did not include these
# keys in their canonical payload.  They remain verifiable against that exact
# historical payload; new records always use the complete payload.
_PRE_NERV_HASH_FIELDS = ("selected_channel", "nerv", "intervention_cost")


def _canonical(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


@dataclass
class AuditRecord:
    seq: int
    event_id: str
    payment_id: str
    attempt_number: int
    source: str
    mode: Optional[str]
    simulation_id: Optional[str]
    timestamp: float
    failure_class: str
    timing_score: Optional[float]
    recommended_window: Optional[str]
    recovery_probability: Optional[float]
    expected_net_recovery: Optional[float]
    ai_recommendation: Dict[str, Any]
    policy_verdict: str
    policy_checks: List[Dict[str, Any]]
    action: str
    execution_result: Optional[Dict[str, Any]]
    outcome: Optional[str]
    model_version: str
    policy_version: str
    prev_hash: str
    current_hash: str = ""
    selected_strategy: Optional[str] = None
    expected_recovery: Optional[float] = None
    expected_utility: Optional[float] = None
    portfolio_rank: Optional[int] = None
    actual_recovered_amount: Optional[float] = None
    selected_channel: Optional[str] = None
    nerv: Optional[float] = None
    intervention_cost: Optional[float] = None
    # Pillar 4: realized utility from the observed outcome + the learning
    # update payload (data_kind, counterfactuals) for the audit chain.
    realized_utility: Optional[float] = None
    learning_update: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AuditLedger:
    """
    In-memory + optional file-backed hash-chained ledger.
    In the FastAPI app this is backed by a DB table; here it's kept
    simple (list + optional JSONL file) so it's runnable with zero deps.
    """

    def __init__(self, path: Optional[str] = None):
        self.path = path
        self.records: List[AuditRecord] = []
        if path:
            self._load()

    def _load(self):
        try:
            with open(self.path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)
                    self.records.append(AuditRecord(**d))
        except FileNotFoundError:
            pass

    def _append_to_file(self, record: AuditRecord):
        if not self.path:
            return
        with open(self.path, "a") as f:
            f.write(_canonical(record.to_dict()) + "\n")

    def append(
        self,
        *,
        event_id: str,
        payment_id: str,
        attempt_number: int,
        source: str,
        mode: Optional[str],
        simulation_id: Optional[str],
        failure_class: str,
        timing_score: Optional[float],
        recommended_window: Optional[str],
        recovery_probability: Optional[float],
        expected_net_recovery: Optional[float],
        ai_recommendation: Dict[str, Any],
        policy_verdict: str,
        policy_checks: List[Dict[str, Any]],
        action: str,
        execution_result: Optional[Dict[str, Any]],
        outcome: Optional[str],
        model_version: str,
        policy_version: str,
        selected_strategy: Optional[str] = None,
        expected_recovery: Optional[float] = None,
        expected_utility: Optional[float] = None,
        portfolio_rank: Optional[int] = None,
        actual_recovered_amount: Optional[float] = None,
        selected_channel: Optional[str] = None,
        nerv: Optional[float] = None,
        intervention_cost: Optional[float] = None,
        realized_utility: Optional[float] = None,
        learning_update: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        prev_hash = self.records[-1].current_hash if self.records else GENESIS_HASH
        seq = len(self.records)

        record = AuditRecord(
            seq=seq,
            event_id=event_id,
            payment_id=payment_id,
            attempt_number=attempt_number,
            source=source,
            mode=mode,
            simulation_id=simulation_id,
            timestamp=time.time(),
            failure_class=failure_class,
            timing_score=timing_score,
            recommended_window=recommended_window,
            recovery_probability=recovery_probability,
            expected_net_recovery=expected_net_recovery,
            ai_recommendation=ai_recommendation,
            policy_verdict=policy_verdict,
            policy_checks=policy_checks,
            action=action,
            execution_result=execution_result,
            outcome=outcome,
            model_version=model_version,
            policy_version=policy_version,
            prev_hash=prev_hash,
            selected_strategy=selected_strategy,
            expected_recovery=expected_recovery,
            expected_utility=expected_utility,
            portfolio_rank=portfolio_rank,
            actual_recovered_amount=actual_recovered_amount,
            selected_channel=selected_channel,
            nerv=nerv,
            intervention_cost=intervention_cost,
            realized_utility=realized_utility,
            learning_update=learning_update,
        )

        payload = record.to_dict()
        payload.pop("current_hash")
        digest_input = prev_hash + _canonical(payload)
        record.current_hash = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()

        self.records.append(record)
        self._append_to_file(record)
        return record

    def verify_chain(self) -> Dict[str, Any]:
        """Walk the chain and confirm every hash is consistent. Returns a report."""
        broken_at: Optional[int] = None
        broken_index: Optional[int] = None
        expected_prev = GENESIS_HASH

        for i, record in enumerate(self.records):
            if record.seq != i:
                broken_at = record.seq
                broken_index = i
                break
            if record.prev_hash != expected_prev:
                broken_at = record.seq
                broken_index = i
                break
            payload = record.to_dict()
            payload.pop("current_hash")
            digest_input = record.prev_hash + _canonical(payload)
            recomputed = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
            if recomputed != record.current_hash:
                # Backward-compatible verification for records whose hashes
                # predate the three NERV channel-economics fields. This is a
                # second SHA-256 verification over the original payload, not
                # an acceptance of an unchecked record.
                legacy_payload = {
                    key: value for key, value in payload.items()
                    if key not in _PRE_NERV_HASH_FIELDS
                }
                legacy_digest = hashlib.sha256(
                    (record.prev_hash + _canonical(legacy_payload)).encode("utf-8")
                ).hexdigest()
                if legacy_digest == record.current_hash:
                    expected_prev = record.current_hash
                    continue
                broken_at = record.seq
                broken_index = i
                break
            expected_prev = record.current_hash

        records_verified = len(self.records) if broken_index is None else broken_index
        if broken_at is None:
            message = f"Verified {records_verified} audit record(s) with SHA-256 integrity."
        else:
            message = f"Audit chain verification failed at sequence {broken_at}."
        return {
            "valid": broken_at is None,
            "n_records": len(self.records),
            "broken_at_seq": broken_at,
            "records_verified": records_verified,
            "message": message,
            "first_invalid_seq": broken_at,
        }

    def to_list(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self.records]


if __name__ == "__main__":
    ledger = AuditLedger()
    ledger.append(
        event_id="evt_1", payment_id="pay_1", attempt_number=1,
        source="PRODUCTION", mode=None, simulation_id=None, failure_class="SOFT", timing_score=0.7,
        recommended_window="18:00-20:00",
        recovery_probability=0.78, expected_net_recovery=9238.0,
        ai_recommendation={"recommended_action": "SCHEDULE_RETRY"},
        policy_verdict="APPROVE", policy_checks=[], action="SCHEDULE_RETRY",
        execution_result={"status": "SCHEDULED"}, outcome=None,
        model_version="recovery-model-v0.1-logreg", policy_version="policy-v0.1",
    )
    ledger.append(
        event_id="evt_2", payment_id="pay_2", attempt_number=1,
        source="PRODUCTION", mode=None, simulation_id=None, failure_class="AMBIGUOUS", timing_score=0.1,
        recommended_window=None,
        recovery_probability=0.2, expected_net_recovery=-5.0,
        ai_recommendation={"recommended_action": "NO_ACTION"},
        policy_verdict="NO_ACTION", policy_checks=[], action="NO_ACTION",
        execution_result=None, outcome=None,
        model_version="recovery-model-v0.1-logreg", policy_version="policy-v0.1",
    )
    print(json.dumps(ledger.verify_chain(), indent=2))

    # tamper demo
    ledger.records[0].expected_net_recovery = 999999.0
    print("after tampering:", json.dumps(ledger.verify_chain(), indent=2))
