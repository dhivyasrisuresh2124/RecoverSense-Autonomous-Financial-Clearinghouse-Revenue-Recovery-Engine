import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import AuditRecordDB

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("")
def list_audit_records(db: Session = Depends(get_db), limit: int = 200, offset: int = 0):
    rows = db.query(AuditRecordDB).order_by(AuditRecordDB.seq.asc()).offset(offset).limit(limit).all()
    return {
        "items": [
            {
                "seq": r.seq, "event_id": r.event_id, "payment_id": r.payment_id,
                "timestamp": r.timestamp, "timing_score": r.timing_score,
                "recovery_probability": r.recovery_probability,
                "expected_net_recovery": r.expected_net_recovery,
                "expected_recovery": r.expected_recovery, "expected_utility": r.expected_utility,
                "selected_strategy": r.selected_strategy, "portfolio_rank": r.portfolio_rank,
                "actual_recovered_amount": r.actual_recovered_amount,
                "policy_verdict": r.policy_verdict, "action": r.action,
                "outcome": r.outcome, "prev_hash": r.prev_hash, "current_hash": r.current_hash,
                "model_version": r.model_version, "policy_version": r.policy_version,
                "failure_class": r.failure_class, "recovery_probability": r.recovery_probability,
                "expected_net_recovery": r.expected_net_recovery,
                "ai_recommendation": r.ai_recommendation_json,
                "policy_checks": r.policy_checks_json, "execution_result": r.execution_result_json,
            }
            for r in rows
        ]
    }


@router.get("/verify")
def verify_chain(db: Session = Depends(get_db)):
    """
    Re-walks the chain stored in the DB and recomputes hashes to confirm
    no record has been altered since it was written. See
    audit/audit_ledger.py for the exact hashing scheme.
    """
    from audit.audit_ledger import AuditRecord, AuditLedger

    rows = db.query(AuditRecordDB).order_by(AuditRecordDB.seq.asc()).all()
    ledger = AuditLedger()
    for r in rows:
        ledger.records.append(AuditRecord(
            seq=r.seq, event_id=r.event_id, payment_id=r.payment_id,
            attempt_number=r.attempt_number, source=r.source, mode=r.mode, simulation_id=r.simulation_id,
            timestamp=r.timestamp, failure_class=r.failure_class,
            timing_score=r.timing_score, recommended_window=r.recommended_window,
            recovery_probability=r.recovery_probability,
            expected_net_recovery=r.expected_net_recovery, ai_recommendation=r.ai_recommendation_json,
            policy_verdict=r.policy_verdict, policy_checks=r.policy_checks_json, action=r.action,
            execution_result=r.execution_result_json, outcome=r.outcome,
            model_version=r.model_version, policy_version=r.policy_version,
            prev_hash=r.prev_hash, current_hash=r.current_hash, selected_strategy=r.selected_strategy,
            expected_recovery=r.expected_recovery, expected_utility=r.expected_utility,
            portfolio_rank=r.portfolio_rank, actual_recovered_amount=r.actual_recovered_amount,
            selected_channel=r.selected_channel, nerv=r.nerv,
            intervention_cost=r.intervention_cost,
            realized_utility=r.realized_utility,
            learning_update=r.learning_update_json,
        ))
    report = ledger.verify_chain()
    return {
        "valid": report["valid"],
        "records_verified": report["records_verified"],
        "message": report["message"],
        "first_invalid_seq": report["first_invalid_seq"],
        "n_records": report["n_records"],
        "broken_at_seq": report["broken_at_seq"],
    }
