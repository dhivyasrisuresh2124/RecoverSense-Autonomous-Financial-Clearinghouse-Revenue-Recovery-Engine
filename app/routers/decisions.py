import sys, os, time, uuid, logging
from typing import Optional
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app.decision_state import is_duplicate_action
from app.models_db import PaymentEventDB, DecisionDB, AuditRecordDB, LedgerHeadDB
from app.schemas import PaymentEventIn
from decision_pipeline import run_decision
from models.recovery_model import RecoveryModel
from execution.razorpay_client import get_default_adapter, get_execution_adapter_for_event
from optimization.recovery_optimizer import RecoveryOpportunity
from policy.policy_engine import PolicyConfig

router = APIRouter(prefix="/decisions", tags=["decisions"])
logger = logging.getLogger(__name__)

# Process-lifetime singletons. The demo loads its versioned development-set
# artifact at startup (or creates it once deterministically when absent).
_recovery_model = RecoveryModel()
_execution_adapter = get_default_adapter()


def load_or_train_demo_model() -> bool:
    """Load the demo artifact, creating it from the seeded dev set once."""
    from pathlib import Path
    from data.generator import SyntheticDataEngine
    from evaluation.evaluate import train_model

    global _recovery_model
    artifact_path = Path(__file__).resolve().parents[2] / "artifacts" / "recovery_model.pkl"
    try:
        if artifact_path.exists():
            try:
                _recovery_model = RecoveryModel().load(str(artifact_path))
                if _recovery_model.trained:
                    return True
            except Exception:
                logger.warning("Demo model artifact is unreadable; rebuilding it from the seeded development set.")

        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        engine = SyntheticDataEngine(seed=42)
        _recovery_model = train_model(engine.generate(3000), engine)
        _recovery_model.save(str(artifact_path))
        return bool(_recovery_model.trained)
    except Exception:
        logger.exception("Could not load or train the demo recovery model.")
        _recovery_model = RecoveryModel()
        return False


def load_production_model(path: str) -> bool:
    global _recovery_model
    try:
        _recovery_model = RecoveryModel().load(path)
        return bool(_recovery_model.trained)
    except Exception:
        return False


def model_readiness() -> str:
    from app.config import get_settings
    return "loaded" if get_settings().PRODUCTION_MODE and _recovery_model.trained else ("ok" if not get_settings().PRODUCTION_MODE else "missing")


def _db_ledger_append(db: Session, **kwargs) -> "AuditRecord":
    """Append to a DB-backed hash chain via an atomically allocated head row.

    Concurrent appends (background agent lifecycles + sync request threads)
    serialize on the head row through a single UPDATE ... RETURNING statement.
    """
    from audit.audit_ledger import AuditRecord, GENESIS_HASH, _canonical
    import hashlib

    # Allocate (seq, prev_hash) with ONE atomic statement. The previous
    # SELECT ... FOR UPDATE + read-increment-write pattern was unsafe on
    # SQLite: FOR UPDATE is silently ignored there and pysqlite only opens a
    # transaction at the first DML, so the head read ran unlocked and
    # concurrent appends collided on UNIQUE(audit_records.seq). The UPDATE is
    # the transaction's first write, so allocators serialize on the row itself;
    # RETURNING yields the pre-update current_hash (the chain's prev_hash)
    # together with the freshly allocated seq. PostgreSQL behaves identically
    # (the row lock is held until commit).
    row = db.execute(
        text("UPDATE audit_ledger_head SET seq = seq + 1 WHERE id = 1 "
             "RETURNING seq, current_hash")
    ).first()
    if row is None:
        # Defensive bootstrap (init_db normally creates the head row).
        db.add(LedgerHeadDB(id=1, seq=0, current_hash=GENESIS_HASH))
        db.flush()
        seq, prev_hash = 0, GENESIS_HASH
    else:
        seq, prev_hash = int(row[0]), row[1]

    record = AuditRecord(
        seq=seq, event_id=kwargs["event_id"], payment_id=kwargs["payment_id"],
        attempt_number=kwargs["attempt_number"], source=kwargs["source"],
        mode=kwargs["mode"], simulation_id=kwargs["simulation_id"],
        timestamp=time.time(), failure_class=kwargs["failure_class"],
        timing_score=kwargs["timing_score"], recommended_window=kwargs["recommended_window"],
        recovery_probability=kwargs["recovery_probability"],
        expected_net_recovery=kwargs["expected_net_recovery"],
        ai_recommendation=kwargs["ai_recommendation"], policy_verdict=kwargs["policy_verdict"],
        policy_checks=kwargs["policy_checks"], action=kwargs["action"],
        execution_result=kwargs["execution_result"], outcome=kwargs["outcome"],
        model_version=kwargs["model_version"], policy_version=kwargs["policy_version"],
        prev_hash=prev_hash,
        selected_strategy=kwargs.get("selected_strategy"), expected_recovery=kwargs.get("expected_recovery"),
        expected_utility=kwargs.get("expected_utility"), portfolio_rank=kwargs.get("portfolio_rank"),
        actual_recovered_amount=kwargs.get("actual_recovered_amount"),
        selected_channel=kwargs.get("selected_channel"), nerv=kwargs.get("nerv"),
        intervention_cost=kwargs.get("intervention_cost"),
        realized_utility=kwargs.get("realized_utility"),
        learning_update=kwargs.get("learning_update"),
    )
    payload = record.to_dict(); payload.pop("current_hash")
    record.current_hash = hashlib.sha256((prev_hash + _canonical(payload)).encode("utf-8")).hexdigest()

    db.add(AuditRecordDB(
        seq=record.seq, event_id=record.event_id, payment_id=record.payment_id,
        attempt_number=record.attempt_number, source=record.source, mode=record.mode,
        simulation_id=record.simulation_id, timestamp=record.timestamp,
        failure_class=record.failure_class, timing_score=record.timing_score,
        recommended_window=record.recommended_window,
        recovery_probability=record.recovery_probability, expected_net_recovery=record.expected_net_recovery,
        ai_recommendation_json=record.ai_recommendation, policy_verdict=record.policy_verdict,
        policy_checks_json=record.policy_checks, action=record.action,
        execution_result_json=record.execution_result, outcome=record.outcome,
        selected_strategy=record.selected_strategy, expected_recovery=record.expected_recovery,
        expected_utility=record.expected_utility, portfolio_rank=record.portfolio_rank,
        actual_recovered_amount=record.actual_recovered_amount,
        selected_channel=record.selected_channel, nerv=record.nerv,
        intervention_cost=record.intervention_cost,
        realized_utility=record.realized_utility,
        learning_update_json=record.learning_update,
        model_version=record.model_version, policy_version=record.policy_version,
        prev_hash=record.prev_hash, current_hash=record.current_hash,
    ))
    # Publish the new head hash with a compare-and-swap on the previous hash.
    # The allocation UPDATE above handed us (seq, prev_hash) while holding the
    # head row's write lock, and that lock is held until this transaction
    # commits, so on SQLite/PostgreSQL the CAS always matches on the happy
    # path. If any driver/isolation change ever allowed a concurrent writer to
    # move the head between allocation and publication, rowcount would be 0
    # and the append fails closed here instead of silently forking the chain.
    head_publication = db.execute(
        text("UPDATE audit_ledger_head SET current_hash = :current_hash "
             "WHERE id = 1 AND current_hash = :prev_hash"),
        {"current_hash": record.current_hash, "prev_hash": prev_hash},
    )
    if head_publication.rowcount != 1:
        raise RuntimeError(
            "Audit ledger head moved during append (concurrent writer detected); "
            "append aborted fail-closed to preserve hash-chain integrity."
        )
    db.flush()
    return record


def _decision_to_response(event_row: PaymentEventDB, decision_row: DecisionDB) -> dict:
    return {
        "event_id": event_row.event_id,
        "payment_id": event_row.payment_id,
        "failure_class": decision_row.failure_class,
        "timing": {
            "liquidity_timing_score": decision_row.timing_score,
            "recommended_window": decision_row.recommended_window,
        },
        "ai_context": decision_row.ai_recommendation_json,
        "recovery": {
            "recovery_probability": decision_row.recovery_probability,
            "model_version": decision_row.model_version,
        },
        "expected_value": {
            "expected_net_recovery": decision_row.expected_net_recovery,
        },
        "policy": {
            "verdict": decision_row.policy_verdict,
            "policy_version": decision_row.policy_version,
            "checks": decision_row.policy_checks_json,
        },
        "execution": decision_row.execution_result_json,
        "audit": {"seq": decision_row.audit_seq, "current_hash": decision_row.audit_hash},
    }


@router.post("/run/{event_id}")
def run_single_decision(event_id: str, db: Session = Depends(get_db),
                        opportunity: Optional[RecoveryOpportunity] = None):
    event_row = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == event_id).first()
    if not event_row:
        raise HTTPException(status_code=404, detail="Event not found")
    existing_decision = db.query(DecisionDB).filter(DecisionDB.event_id == event_id).first()
    if existing_decision:
        return _decision_to_response(event_row, existing_decision)

    event_dict = {
        "event_id": event_row.event_id, "customer_id": event_row.customer_id,
        "payment_id": event_row.payment_id, "subscription_id": event_row.subscription_id,
        "amount": event_row.amount, "currency": event_row.currency,
        "timestamp": event_row.timestamp, "failure_reason": event_row.failure_reason,
        "failure_class": event_row.failure_class, "attempt_number": event_row.attempt_number,
        "mandate_status": event_row.mandate_status, "payment_status": event_row.payment_status,
        "source": event_row.source, "mode": event_row.mode, "simulation_id": event_row.simulation_id,
        "metadata": event_row.metadata_json or {},
    }
    from agent.context_reasoner import get_context_recommendation
    from models.timing_model import compute_timing_score

    timing_preview = compute_timing_score(event_dict["metadata"].get("history", []))
    context_preview = get_context_recommendation(
        support_note=event_dict["metadata"].get("support_note", ""),
        failure_class=event_dict["failure_class"],
        timing_window=timing_preview.to_dict()["recommended_window"],
    )
    duplicate_result = is_duplicate_action(
        db,
        payment_id=event_row.payment_id,
        attempt_number=event_row.attempt_number,
        recommended_action=context_preview.recommended_action,
        source=event_row.source,
        mode=event_row.mode,
        dedupe_window_minutes=PolicyConfig().dedupe_window_minutes,
    )

    class _DBBackedLedger:
        """Adapter so decision_pipeline.run_decision() can call .append(**kwargs)
        without knowing it's writing to a SQL table under the hood."""
        def append(self, **kwargs):
            return _db_ledger_append(db, **kwargs)

    execution_adapter = get_execution_adapter_for_event(
        event_row.source, event_row.mode, simulator_adapter=_execution_adapter
    )
    output = run_decision(
        event_dict,
        recovery_model=_recovery_model,
        execution_adapter=execution_adapter,
        audit_ledger=_DBBackedLedger(),
        already_actioned_recently=duplicate_result.is_duplicate,
    )

    logger.info(
        "decision_trace payment_id=%s event_id=%s attempt_number=%s failure_class=%s "
        "recommended_action=%s expected_net_recovery=%.2f recommended_window=%s "
        "already_actioned_recently=%s failed_policy_checks=%s policy_verdict=%s execution_status=%s "
        "adapter=%s capability=%s",
        output.payment_id,
        output.event_id,
        event_row.attempt_number,
        output.failure_class,
        output.ai_context.recommended_action,
        output.expected_value.expected_net_recovery,
        output.ai_context.recommended_window,
        duplicate_result.is_duplicate,
        [c.name for c in output.policy.checks if not c.passed],
        output.policy.verdict,
        output.execution.status if output.execution else "NOT_EXECUTED",
        execution_adapter.name,
        output.execution.capability if output.execution and getattr(output.execution, "capability", None) else "N/A",
    )

    decision_row = DecisionDB(
        event_id=output.event_id, payment_id=output.payment_id, customer_id=event_row.customer_id,
        amount=event_row.amount, attempt_number=event_row.attempt_number, failure_class=output.failure_class,
        timing_score=output.timing.liquidity_timing_score,
        recommended_window=output.timing.to_dict()["recommended_window"],
        recovery_probability=output.recovery.probability,
        expected_net_recovery=output.expected_value.expected_net_recovery,
        ai_recommendation_json=output.ai_context.to_dict(),
        policy_verdict=output.policy.verdict,
        policy_checks_json=[c.__dict__ for c in output.policy.checks],
        action=(output.execution.status if output.execution else output.policy.verdict),
        execution_result_json=output.execution.to_dict() if output.execution else None,
        source=event_row.source, mode=event_row.mode, simulation_id=event_row.simulation_id,
        model_version=output.recovery.model_version, policy_version=output.policy.policy_version,
        audit_seq=output.audit_record.seq, audit_hash=output.audit_record.current_hash,
        # Pillar 4 data-flow: when this decision was driven by an optimizer
        # RecoveryOpportunity, persist the optimizer-selected strategy so the
        # outcome/learning path (record_outcome) can attribute observed outcomes
        # to the actual chosen strategy. When no opportunity is supplied (e.g. a
        # direct /decisions/run call), these fields remain NULL exactly as before.
        selected_strategy=opportunity.selected_strategy if opportunity else None,
        selected_channel=opportunity.selected_channel if opportunity else None,
        nerv=opportunity.nerv if opportunity else None,
        intervention_cost=opportunity.intervention_cost if opportunity else None,
        expected_recovery=opportunity.expected_recovery if opportunity else None,
        expected_utility=opportunity.expected_utility if opportunity else None,
        alternatives_json=opportunity.alternatives if opportunity else None,
        portfolio_rank=opportunity.rank if opportunity else None,
        optimization_cycle_id=getattr(opportunity, "optimization_cycle_id", None),
    )
    db.add(decision_row)
    db.commit()

    return output.to_dict()


@router.post("/run-all")
def run_all_pending(db: Session = Depends(get_db)):
    """Backward-compatible bulk endpoint, now backed by a portfolio action budget."""
    from app.routers.optimization import run_optimization_cycle
    return run_optimization_cycle(max_actions_per_cycle=10, db=db)


@router.post("/simulate-one")
def simulate_one(event_in: PaymentEventIn, db: Session = Depends(get_db)):
    """
    Used by the Simulator UI: accepts a single, user-authored event
    (custom amount / failure reason / synthetic history / support note),
    persists it, runs the full decision pipeline immediately, and
    returns the decision output — all in one call.
    """
    from models.failure_classifier import classify_failure

    source = (event_in.source or "SIMULATOR").upper()
    mode = (event_in.mode or "SIMULATION").upper()
    event_id = f"evt_sim_{uuid.uuid4().hex[:16]}"
    simulation_id = event_in.simulation_id or event_in.event_id

    row = PaymentEventDB(
        event_id=event_id, customer_id=event_in.customer_id,
        payment_id=event_in.payment_id, subscription_id=event_in.subscription_id,
        amount=event_in.amount, currency=event_in.currency, timestamp=event_in.timestamp,
        failure_reason=event_in.failure_reason,
        failure_class=classify_failure(event_in.failure_reason),
        attempt_number=event_in.attempt_number, mandate_status=event_in.mandate_status,
        payment_status=event_in.payment_status, source=source, mode=mode,
        simulation_id=simulation_id, metadata_json=event_in.metadata,
    )
    db.add(row)
    db.commit()

    response = run_single_decision(event_id, db)
    response["simulation"] = {"source": "SIMULATION", "input": "Synthetic input", "id": simulation_id}
    return response


@router.get("/status")
def ai_reasoner_status():
    """
    Reports whether the AI Context Reasoner is currently configured to
    call a real LLM or is running on the transparent rule-based fallback.
    Use this before a demo/pitch to confirm which path will actually
    fire — 'source' in every decision's ai_context also reports this
    per-decision, but this endpoint lets you check it up front without
    running a decision.
    """
    from app.config import get_settings
    settings = get_settings()
    configured = bool(settings.ANTHROPIC_API_KEY)
    try:
        import anthropic  # noqa: F401
        sdk_available = True
    except ImportError:
        sdk_available = False

    return {
        "llm_configured": configured,
        "anthropic_sdk_installed": sdk_available,
        "active_path": "llm" if (configured and sdk_available) else "fallback_rule_based",
        "note": (
            "Every decision's ai_context.source field reports which path actually ran "
            "for that specific decision, since a configured LLM call can still fail at "
            "runtime and fall back safely — this endpoint reports the static configuration."
        ),
    }


@router.post("/train")
def train_recovery_model(n_dev: int = 3000, seed: int = 42):
    """Trains the recovery probability model on a fresh synthetic dev set."""
    from data.generator import SyntheticDataEngine
    from evaluation.evaluate import train_model

    global _recovery_model
    engine = SyntheticDataEngine(seed=seed)
    dev_events = engine.generate(n_dev)
    _recovery_model = train_model(dev_events, engine)
    return {"trained": _recovery_model.trained, "n_dev_events": n_dev, "model_version": getattr(_recovery_model, "artifact_fingerprint", lambda: "unavailable")()}
