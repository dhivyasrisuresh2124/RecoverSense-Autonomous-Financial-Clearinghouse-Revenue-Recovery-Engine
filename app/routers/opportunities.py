import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import DecisionDB, PaymentEventDB
from models.timing_model import compute_timing_score
from agent.context_reasoner import get_context_recommendation

router = APIRouter(prefix="/opportunities", tags=["opportunities"])


def _provider_evidence(event: PaymentEventDB) -> dict:
    """Return only provider facts causally available on this payment event."""
    metadata = event.metadata_json or {}
    raw_event = str(metadata.get("raw_webhook_event") or "")
    # The event row is authoritative.  Some persisted Razorpay event payloads
    # retain the source payment entity in metadata; this fallback is still the
    # same payment event, never a subscription/amount-based lookup.
    entity = metadata.get("entity") or metadata.get("payment_entity") or metadata.get("raw_payment_entity") or {}
    subscription_id = event.subscription_id or metadata.get("subscription_id") or entity.get("subscription_id")
    return {
        "provider": "razorpay" if raw_event.startswith("payment.") or raw_event.startswith("subscription.") else None,
        "environment": "test", "rail": metadata.get("rail"),
        "mandate_status": event.mandate_status, "subscription_id": subscription_id,
        "previous_payment_id": None, "provider_state": event.payment_status,
        "payment_method": metadata.get("method"), "amount": event.amount, "mandate_token_verified": False,
        "mandate_token": None,
        "execution_note": "Event-linked provider evidence only. No new RecoverSense recovery debit is claimed.",
    }


@router.get("")
def list_opportunities(
    db: Session = Depends(get_db),
    q: Optional[str] = Query(None, description="search customer_id or payment_id"),
    failure_class: Optional[str] = None,
    policy_verdict: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    query = db.query(DecisionDB)
    if q:
        like = f"%{q}%"
        query = query.filter((DecisionDB.customer_id.like(like)) | (DecisionDB.payment_id.like(like)))
    if failure_class:
        query = query.filter(DecisionDB.failure_class == failure_class)
    if policy_verdict:
        query = query.filter(DecisionDB.policy_verdict == policy_verdict)

    total = query.count()
    rows = query.order_by(DecisionDB.created_at.desc()).offset(offset).limit(limit).all()

    return {
        "total": total,
        "items": [
            {
                "event_id": r.event_id, "payment_id": r.payment_id, "customer_id": r.customer_id,
                "amount": r.amount, "attempt_number": r.attempt_number, "failure_class": r.failure_class,
                "timing_score": r.timing_score, "recommended_window": r.recommended_window,
                "recovery_probability": r.recovery_probability,
                "expected_net_recovery": r.expected_net_recovery,
                "expected_recovery": r.expected_recovery,
                "expected_utility": r.expected_utility,
                "selected_strategy": r.selected_strategy,
                "portfolio_rank": r.portfolio_rank,
                "outcome": r.outcome,
                "actual_recovered_amount": r.actual_recovered_amount,
                "action": r.action, "policy_verdict": r.policy_verdict,
                "source": r.source, "mode": r.mode,
                "created_at": str(r.created_at),
            }
            for r in rows
        ],
    }


@router.get("/{event_id}")
def get_opportunity_detail(event_id: str, db: Session = Depends(get_db)):
    decision = db.query(DecisionDB).filter(DecisionDB.event_id == event_id).first()
    event = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == event_id).first()
    if not decision or not event:
        raise HTTPException(status_code=404, detail="Not found")

    history = (event.metadata_json or {}).get("history", [])
    timing = compute_timing_score(history)
    ai_context = get_context_recommendation(
        support_note=(event.metadata_json or {}).get("support_note", ""),
        failure_class=event.failure_class,
        timing_window=timing.to_dict()["recommended_window"],
    )
    from app.routers.decisions import _recovery_model
    recovery = _recovery_model.predict(
        timing_score=timing.liquidity_timing_score,
        scheduled_hour=timing.circular_mean_hour,
        circular_mean_hour=timing.circular_mean_hour,
        n_history_points=timing.n_history_points,
        failure_class=event.failure_class,
        attempt_number=event.attempt_number,
        high_intent_signal=(ai_context.customer_intent == "HIGH"),
        failure_reason=event.failure_reason,
    )

    return {
        "event": {
            "event_id": event.event_id, "customer_id": event.customer_id,
            "payment_id": event.payment_id, "amount": event.amount,
            "failure_reason": event.failure_reason, "failure_class": event.failure_class,
            "attempt_number": event.attempt_number, "timestamp": event.timestamp,
            "history": (event.metadata_json or {}).get("history", []),
            "support_note": (event.metadata_json or {}).get("support_note", ""),
        },
        "decision": {
            "timing_score": decision.timing_score, "recommended_window": decision.recommended_window,
            "recovery_probability": decision.recovery_probability,
            "recovery": recovery.to_dict(),
            "expected_net_recovery": decision.expected_net_recovery,
            "expected_recovery": decision.expected_recovery,
            "expected_utility": decision.expected_utility,
            "selected_strategy": decision.selected_strategy,
            "selected_channel": decision.selected_channel,
            "nerv": decision.nerv,
            "intervention_cost": decision.intervention_cost,
            "alternatives": decision.alternatives_json,
            "portfolio_rank": decision.portfolio_rank,
            "optimization_cycle_id": decision.optimization_cycle_id,
            "ai_recommendation": decision.ai_recommendation_json,
            "policy_verdict": decision.policy_verdict, "policy_checks": decision.policy_checks_json,
            "action": decision.action, "execution_result": decision.execution_result_json,
            "attempt_number": decision.attempt_number, "source": decision.source, "mode": decision.mode,
            "model_version": decision.model_version, "policy_version": decision.policy_version,
            "audit_seq": decision.audit_seq, "audit_hash": decision.audit_hash,
            "outcome": decision.outcome, "actual_recovered_amount": decision.actual_recovered_amount,
        },
        "provider_evidence": _provider_evidence(event),
    }
