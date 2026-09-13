"""Portfolio optimization and closed-loop outcome APIs."""
import sys, os, uuid
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text, func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import (DecisionDB, PaymentEventDB, OptimizationCycleDB,
    OptimizationCycleSequenceDB, PortfolioOpportunityDB, StrategyPerformanceDB,
    ProviderOutcomeEvidenceDB, ProviderRefundDB)
from app.decision_state import is_duplicate_action
from models.timing_model import compute_timing_score
from models.failure_classifier import classify_failure
from agent.context_reasoner import get_context_recommendation
from policy.policy_engine import PolicyConfig
from optimization.recovery_optimizer import (OPTIMIZER_VERSION, StrategyPerformance,
    optimize_portfolio, record_strategy_outcome)

router = APIRouter(prefix="/optimization", tags=["optimization"])


def _next_cycle_sequence(db: Session) -> int:
    """Allocate a persisted monotonic sequence atomically.

    A single UPDATE ... RETURNING on the sequence row serializes concurrent
    allocators: SELECT ... FOR UPDATE is a no-op on SQLite and pysqlite only
    opens a transaction at the first DML, so the previous read-increment-write
    raced under concurrent cycles. PostgreSQL holds the row lock until commit.
    """
    row = db.execute(
        text("UPDATE optimization_cycle_sequence SET current_sequence = current_sequence + 1 "
             "WHERE id = 1 RETURNING current_sequence")
    ).first()
    if row is None:
        # Defensive bootstrap (init_db normally creates the sequence row).
        db.add(OptimizationCycleSequenceDB(id=1, current_sequence=1))
        db.flush()
        return 1
    return int(row[0])


def _latest_cycle(db: Session):
    return db.query(OptimizationCycleDB).order_by(
        OptimizationCycleDB.sequence.desc(),
        OptimizationCycleDB.created_at.desc(),
        OptimizationCycleDB.id.desc(),
    ).first()


def financial_outcome_metrics(db: Session) -> dict:
    """Provider-verified financial facts; unmatched captures are excluded."""
    gross = db.query(func.coalesce(func.sum(ProviderOutcomeEvidenceDB.amount), 0.0)).filter(
        ProviderOutcomeEvidenceDB.reconciliation_status == "RECOVERED"
    ).scalar()
    refunded = db.query(func.coalesce(func.sum(ProviderRefundDB.amount), 0.0)).filter(
        ProviderRefundDB.provider_status == "PROCESSED",
        ProviderRefundDB.matched_event_id.isnot(None),
    ).scalar()
    gross, refunded = float(gross or 0.0), float(refunded or 0.0)
    return {"gross_recovered_revenue": round(gross, 2), "refunded_revenue": round(refunded, 2),
            "net_recovered_revenue": round(gross - refunded, 2)}


class OutcomeIn(BaseModel):
    outcome: str = Field(pattern="^(RECOVERED|FAILED|DUPLICATE_PREVENTED|TIMEOUT|DEFERRED|ESCALATED|UNRESOLVED)$")
    actual_recovered_amount: float = Field(default=0.0, ge=0.0)


def _event_dict(row: PaymentEventDB):
    return {"event_id": row.event_id, "customer_id": row.customer_id, "payment_id": row.payment_id,
        "subscription_id": row.subscription_id, "amount": row.amount, "currency": row.currency,
        "timestamp": row.timestamp, "failure_reason": row.failure_reason, "failure_class": row.failure_class,
        "attempt_number": row.attempt_number, "mandate_status": row.mandate_status,
        "payment_status": row.payment_status, "source": row.source, "mode": row.mode,
        "simulation_id": row.simulation_id, "metadata": row.metadata_json or {}}


def _analysis(event, model):
    failure_class = event.get("failure_class") or classify_failure(event["failure_reason"])
    timing = compute_timing_score(event.get("metadata", {}).get("history", []))
    context = get_context_recommendation(event.get("metadata", {}).get("support_note", ""), failure_class,
                                         timing.to_dict()["recommended_window"])
    prediction = model.predict(timing_score=timing.liquidity_timing_score,
        scheduled_hour=timing.circular_mean_hour, circular_mean_hour=timing.circular_mean_hour,
        n_history_points=timing.n_history_points, failure_class=failure_class,
        attempt_number=event.get("attempt_number", 1), high_intent_signal=context.customer_intent == "HIGH",
        failure_reason=event["failure_reason"])
    return {"event": event, "failure_class": failure_class, "timing_score": timing.liquidity_timing_score,
            "timing_window": timing.to_dict()["recommended_window"], "recovery_probability": prediction.probability}


def _performance_map(db):
    return {f"{p.strategy}:{p.context_key}": StrategyPerformance(p.strategy, p.context_key, p.attempts,
        p.successful_recoveries, p.expected_recovery_total, p.actual_recovered_total,
        getattr(p, "realized_utility_total", 0.0) or 0.0)
        for p in db.query(StrategyPerformanceDB).all()}


def _initial_allocation_status(opportunity):
    if opportunity.allocated:
        return "ACTION_SELECTED"
    if opportunity.selected_strategy == "ESCALATE":
        return "ESCALATED"
    if opportunity.selected_strategy == "STOP":
        return "STOPPED"
    if opportunity.selected_strategy == "DEFER_AND_RETRY":
        budget_deferred = any("action budget was allocated" in message for message in opportunity.explanation)
        return "UNALLOCATED" if budget_deferred else "DEFERRED"
    return "DEFERRED"


def _portfolio_item(row):
    return {"event_id": row.event_id, "payment_id": row.payment_id, "rank": row.portfolio_rank,
        "amount_at_risk": row.amount, "recovery_probability": row.recovery_probability,
        "expected_recovery": row.expected_recovery, "expected_utility": row.expected_utility,
        "selected_channel": row.selected_channel, "nerv": row.nerv,
        "intervention_cost": row.intervention_cost,
        "selected_strategy": row.selected_strategy, "allocation_status": row.allocation_status,
        "policy_status": row.policy_verdict, "policy_checks": row.policy_checks_json,
        "execution_status": row.execution_status, "execution_result": row.execution_result_json,
        "outcome": row.outcome, "actual_recovered_amount": row.actual_recovered_amount,
        "alternatives": row.alternatives_json, "explanation": row.explanation_json,
        "optimizer_version": row.optimizer_version, "strategy_version": row.strategy_version,
        "created_at": str(row.created_at)}


@router.post("/cycles")
def run_optimization_cycle(max_actions_per_cycle: int = Query(10, ge=0, le=500), db: Session = Depends(get_db)):
    """Allocate a finite action budget to the pending failed-payment portfolio."""
    from app.routers.decisions import _recovery_model, run_single_decision
    # An event is evaluated once per prototype lifecycle. A new payment-attempt event
    # can enter a later cycle, but rerunning a cycle cannot create duplicate portfolio facts.
    considered_ids = [r[0] for r in db.query(PortfolioOpportunityDB.event_id).distinct().all()]
    query = db.query(PaymentEventDB)
    if considered_ids:
        query = query.filter(~PaymentEventDB.event_id.in_(considered_ids))
    rows = query.order_by(PaymentEventDB.timestamp.asc()).limit(500).all()
    if not rows:
        latest = _latest_cycle(db)
        return {"status": "no_new_opportunities", "cycle_id": latest.id if latest else None,
                "processed": 0, "total_opportunities_considered": 0, "opportunities": []}
    opportunities = optimize_portfolio([_analysis(_event_dict(row), _recovery_model) for row in rows],
        max_actions_per_cycle=max_actions_per_cycle, performance=_performance_map(db))
    cycle_id = f"cyc_{uuid.uuid4().hex[:12]}"
    selected = [o for o in opportunities if o.allocated]
    db.add(OptimizationCycleDB(id=cycle_id, sequence=_next_cycle_sequence(db), max_actions=max_actions_per_cycle,
        revenue_at_risk=sum(o.event["amount"] for o in opportunities),
        expected_recoverable=sum(o.expected_recovery for o in opportunities),
        expected_selected_recovery=sum(o.expected_recovery for o in selected),
        expected_nerv=sum(o.nerv or 0.0 for o in selected),
        intervention_spend=sum(o.intervention_cost or 0.0 for o in selected),
        actions_recommended=len(selected), actions_authorized=0, actions_blocked=0))
    # Persist an optimization decision for every considered opportunity before any
    # execution is attempted. These records are not execution decisions.
    for opportunity in opportunities:
        db.add(PortfolioOpportunityDB(optimization_cycle_id=cycle_id, event_id=opportunity.event["event_id"],
            payment_id=opportunity.event["payment_id"], customer_id=opportunity.event.get("customer_id"),
            amount=opportunity.event["amount"], recovery_probability=opportunity.recovery_probability,
            expected_recovery=opportunity.expected_recovery, expected_utility=opportunity.expected_utility,
            selected_channel=opportunity.selected_channel, nerv=opportunity.nerv,
            intervention_cost=opportunity.intervention_cost,
            portfolio_rank=opportunity.rank, selected_strategy=opportunity.selected_strategy,
            alternatives_json=opportunity.alternatives, allocation_status=_initial_allocation_status(opportunity),
            explanation_json=opportunity.explanation, optimizer_version=OPTIMIZER_VERSION,
            strategy_version="strategy-set-v2.0"))
    db.commit()
    authorized = blocked = 0
    for opportunity in selected:
        # Existing pipeline remains the only policy/execution path.
        # The optimizer-selected strategy now flows into DecisionDB at row
        # creation (run_single_decision persists it from this same opportunity
        # object), so the outcome/learning path can attribute observed outcomes.
        opportunity.optimization_cycle_id = cycle_id
        run_single_decision(opportunity.event["event_id"], db, opportunity=opportunity)
        decision = db.query(DecisionDB).filter(DecisionDB.event_id == opportunity.event["event_id"]).first()
        portfolio_row = db.query(PortfolioOpportunityDB).filter(
            PortfolioOpportunityDB.optimization_cycle_id == cycle_id,
            PortfolioOpportunityDB.event_id == opportunity.event["event_id"]).one()
        if decision:
            if decision.policy_verdict == "APPROVE":
                authorized += 1
                portfolio_row.allocation_status = "ACTION_SELECTED"
            else:
                blocked += 1
                portfolio_row.allocation_status = "POLICY_BLOCKED"
            portfolio_row.policy_verdict = decision.policy_verdict
            portfolio_row.policy_checks_json = decision.policy_checks_json
            portfolio_row.execution_status = decision.action if decision.policy_verdict == "APPROVE" else None
            portfolio_row.execution_result_json = decision.execution_result_json
    cycle = db.get(OptimizationCycleDB, cycle_id)
    cycle.actions_authorized, cycle.actions_blocked = authorized, blocked
    db.commit()
    persisted = db.query(PortfolioOpportunityDB).filter(PortfolioOpportunityDB.optimization_cycle_id == cycle_id).all()
    counts = {status: sum(p.allocation_status == status for p in persisted) for status in
              ("ACTION_SELECTED", "DEFERRED", "UNALLOCATED", "ESCALATED", "STOPPED", "POLICY_BLOCKED")}
    return {"cycle_id": cycle_id, "processed": len(selected), "max_actions_per_cycle": max_actions_per_cycle,
        "total_revenue_at_risk": round(sum(o.event["amount"] for o in opportunities), 2),
        "expected_recoverable_revenue": round(sum(o.expected_recovery for o in opportunities), 2),
        "expected_gross_recovery": round(sum(o.expected_recovery for o in selected), 2),
        "expected_nerv": round(sum(o.nerv or 0.0 for o in selected), 2),
        "intervention_spend": round(sum(o.intervention_cost or 0.0 for o in selected), 2),
        "voice_usage": sum(o.selected_channel == "VOICE" for o in selected),
        "expected_recovery_from_selected_actions": round(sum(o.expected_recovery for o in selected), 2),
        "total_opportunities_considered": len(opportunities), "actions_recommended": len(selected),
        "actions_authorized": authorized, "actions_blocked_or_deferred": len(opportunities) - authorized,
        "status_counts": counts, "opportunities": [_portfolio_item(p) for p in sorted(persisted, key=lambda p: (p.portfolio_rank, p.payment_id))]}


@router.get("/portfolio")
def portfolio(db: Session = Depends(get_db)):
    cycle = _latest_cycle(db)
    if not cycle:
        return {"status": "no_optimization_cycle_yet", "items": [], **financial_outcome_metrics(db)}
    rows = db.query(PortfolioOpportunityDB).filter(PortfolioOpportunityDB.optimization_cycle_id == cycle.id).order_by(PortfolioOpportunityDB.portfolio_rank, PortfolioOpportunityDB.payment_id).all()
    statuses = {status: sum(row.allocation_status == status for row in rows) for status in
                ("ACTION_SELECTED", "DEFERRED", "UNALLOCATED", "ESCALATED", "STOPPED", "POLICY_BLOCKED")}
    return {"cycle_id": cycle.id, "total_revenue_at_risk": cycle.revenue_at_risk,
        "expected_recoverable_revenue": cycle.expected_recoverable, "expected_gross_recovery": cycle.expected_selected_recovery,
        "expected_nerv": cycle.expected_nerv, "intervention_spend": cycle.intervention_spend,
        "actual_recovered_revenue": cycle.actual_recovered,
        "realized_net_recovery": round(cycle.actual_recovered - cycle.intervention_spend, 2),
        "voice_usage": sum(row.selected_channel == "VOICE" for row in rows),
        "actions_recommended": cycle.actions_recommended, "actions_authorized": cycle.actions_authorized,
        "total_opportunities_considered": len(rows), "status_counts": statuses,
        "items": [_portfolio_item(row) for row in rows], **financial_outcome_metrics(db)}


@router.post("/outcomes/{event_id}")
def record_outcome(event_id: str, body: OutcomeIn, db: Session = Depends(get_db)):
    decision = db.query(DecisionDB).filter(DecisionDB.event_id == event_id).first()
    if not decision:
        raise HTTPException(status_code=404, detail="Decision not found")
    if decision.outcome:
        return {"status": "already_recorded", "outcome": decision.outcome, "actual_recovered_amount": decision.actual_recovered_amount}
    if body.actual_recovered_amount > decision.amount:
        raise HTTPException(status_code=422, detail="Recovered amount cannot exceed amount at risk")
    decision.outcome, decision.actual_recovered_amount, decision.outcome_timestamp = body.outcome, body.actual_recovered_amount, datetime.utcnow()
    portfolio_rows = db.query(PortfolioOpportunityDB).filter(PortfolioOpportunityDB.event_id == event_id).all()
    for portfolio_row in portfolio_rows:
        portfolio_row.outcome = body.outcome
        portfolio_row.actual_recovered_amount = body.actual_recovered_amount
    learning_record = None
    if decision.selected_strategy:
        # Pillar 4 — closed-loop learning: compute REALIZED utility from the
        # observable economic outcome, persist one OBSERVED_OUTCOME learning
        # update plus ESTIMATED_COUNTERFACTUAL rows for every non-selected
        # strategy, and update the EXISTING strategy-performance statistics.
        from learning.closed_loop import apply_observed_outcome
        alternatives = decision.alternatives_json or [
            row.alternatives_json for row in portfolio_rows if row.alternatives_json
        ] or []
        if alternatives and isinstance(alternatives[0], list):
            # PortfolioOpportunityDB rows carry raw optimizer alternative lists.
            alternatives = alternatives[0]
        learning_record = apply_observed_outcome(
            db=db,
            event_id=event_id,
            payment_id=decision.payment_id,
            strategy=decision.selected_strategy,
            context_key=(decision.failure_class or "AMBIGUOUS").upper(),
            outcome=body.outcome,
            expected_utility=decision.expected_utility or 0.0,
            expected_recovery=decision.expected_recovery or 0.0,
            amount_at_risk=decision.amount,
            actual_recovered=body.actual_recovered_amount,
            alternatives=alternatives,
        )
    if decision.optimization_cycle_id:
        cycle = db.get(OptimizationCycleDB, decision.optimization_cycle_id)
        if cycle: cycle.actual_recovered += body.actual_recovered_amount
    # Outcomes are appended as new audit facts; historical decision records are never mutated.
    event = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == event_id).first()
    if event:
        from app.routers.decisions import _db_ledger_append
        audit = _db_ledger_append(db, event_id=event_id, payment_id=decision.payment_id,
            attempt_number=decision.attempt_number, source=decision.source, mode=decision.mode,
            simulation_id=decision.simulation_id, failure_class=decision.failure_class,
            timing_score=decision.timing_score, recommended_window=decision.recommended_window,
            recovery_probability=decision.recovery_probability, expected_net_recovery=decision.expected_net_recovery,
            ai_recommendation=decision.ai_recommendation_json or {}, policy_verdict=decision.policy_verdict,
            policy_checks=decision.policy_checks_json or [], action="OUTCOME_RECORDED", execution_result=None,
            outcome=body.outcome, model_version=decision.model_version or "unknown",
            policy_version=decision.policy_version or "unknown", selected_strategy=decision.selected_strategy,
            expected_recovery=decision.expected_recovery, expected_utility=decision.expected_utility,
            portfolio_rank=decision.portfolio_rank, actual_recovered_amount=body.actual_recovered_amount,
            selected_channel=decision.selected_channel, nerv=decision.nerv,
            intervention_cost=decision.intervention_cost,
            realized_utility=(learning_record.realized_utility if learning_record else None),
            learning_update=(learning_record.to_dict() if learning_record else None))
    db.commit()
    return {"status": "recorded", "event_id": event_id, "outcome": body.outcome,
            "actual_recovered_amount": body.actual_recovered_amount,
            "realized_utility": (learning_record.realized_utility if learning_record else None),
            "data_kind": (learning_record.data_kind if learning_record else None)}


@router.get("/strategy-performance")
def strategy_performance(db: Session = Depends(get_db)):
    items = []
    for row in db.query(StrategyPerformanceDB).order_by(StrategyPerformanceDB.strategy, StrategyPerformanceDB.context_key):
        p = StrategyPerformance(row.strategy, row.context_key, row.attempts, row.successful_recoveries,
            row.expected_recovery_total, row.actual_recovered_total)
        items.append({"strategy": row.strategy, "context": row.context_key, "attempts": row.attempts,
            "successful_recoveries": row.successful_recoveries, "recovery_rate": round(p.smoothed_rate, 4),
            "expected_recovery": round(row.expected_recovery_total, 2), "actual_recovered": round(row.actual_recovered_total, 2),
            "realized_utility_total": round(getattr(row, "realized_utility_total", 0.0) or 0.0, 2),
            "calibration_ratio": round(p.calibration_ratio, 4)})
    return {"items": items, "learning": "Smoothed strategy outcomes influence future utility after at least three attempts."}
