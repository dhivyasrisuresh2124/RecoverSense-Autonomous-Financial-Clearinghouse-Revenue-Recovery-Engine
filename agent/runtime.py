"""
RecoverSense — Autonomous Agent Runtime
========================================

Orchestrates the 12-stage autonomous lifecycle for payment recovery:

    REAL RAZORPAY WEBHOOK
            ↓
    EVENT_RECEIVED
            ↓
    OPPORTUNITY_CREATED
            ↓
    FAILURE_DIAGNOSED
            ↓
    UNDERWRITING_COMPLETED
            ↓
    STRATEGIES_EVALUATED
            ↓
    ACTION_SELECTED
            ↓
    POLICY_CHECK
            ↓
    PROVIDER_STATE_VERIFIED
            ↓
    EXECUTION_ATTEMPTED
            ↓
    OUTCOME_OBSERVED
            ↓
    LEARNING_UPDATED
            ↓
    AUDIT_COMMITTED
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

_current_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.dirname(_current_dir)
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from agent.context_reasoner import ContextReasonerOutput, get_context_recommendation
from app.decision_state import is_duplicate_action
from app.models_db import (
    DecisionDB,
    FlashSettleExposureDB,
    MultiRailDecisionDB,
    PaymentEventDB,
    YieldCurveDecisionDB,
)
from audit.audit_ledger import AuditLedger, AuditRecord
from execution.razorpay_client import (
    ExecutionAdapter,
    ExecutionResult,
    ProviderCapability,
    get_execution_adapter_for_event,
    normalize_payment_state,
)
from execution.razorpay_upi_autopay import attempt_real_upi_autopay_execution
from execution.voice_adapter import VoiceExecutionAdapter, get_default_voice_adapter
from flash_settle.underwriting import UnderwritingInputs, underwrite as flash_settle_underwrite
from flash_settle.ledger import create_flash_settle_ledger
from yield_curve.underwriting import (
    YieldCurveInputs,
    evaluate_yield_curve as evaluate_yield_curve_underwriting,
)
from yield_curve.ledger import (
    authorize_partial_recovery,
    create_yield_curve_ledger,
    record_partial_recovery,
    schedule_remaining_balance,
)
from multi_rail.engine import (
    MultiRailInputs,
    evaluate_multi_rail as evaluate_multi_rail_engine,
)
from multi_rail.ledger import (
    authorize_rail_switch,
    await_rail_outcome,
    complete_multi_rail,
    create_multi_rail_ledger,
    observe_rail_outcome,
    record_rail_switch,
)
from models.expected_value import CostAssumptions, compute_expected_net_recovery
from models.failure_classifier import classify_failure
from models.recovery_model import RecoveryModel
from models.timing_model import compute_timing_score
from optimization.recovery_optimizer import (
    ACTIONABLE_STRATEGIES,
    RecoveryOpportunity,
    optimize_portfolio,
)
from policy.policy_engine import PolicyConfig, PolicyDecision, evaluate_policy
from policy.safety_gate import (
    DISPOSITION_NEVER_RETRY,
    DISPOSITION_SAFE_TO_RETRY,
    SafetyGateContext,
    evaluate_safety_gate,
)

logger = logging.getLogger("recoversense.agent")

STAGE_EVENT_RECEIVED = "EVENT_RECEIVED"
STAGE_OPPORTUNITY_CREATED = "OPPORTUNITY_CREATED"
STAGE_FAILURE_DIAGNOSED = "FAILURE_DIAGNOSED"
STAGE_UNDERWRITING_COMPLETED = "UNDERWRITING_COMPLETED"
STAGE_STRATEGIES_EVALUATED = "STRATEGIES_EVALUATED"
STAGE_ACTION_SELECTED = "ACTION_SELECTED"
STAGE_POLICY_CHECK = "POLICY_CHECK"
STAGE_PROVIDER_STATE_VERIFIED = "PROVIDER_STATE_VERIFIED"
STAGE_EXECUTION_ATTEMPTED = "EXECUTION_ATTEMPTED"
STAGE_OUTCOME_OBSERVED = "OUTCOME_OBSERVED"
STAGE_LEARNING_UPDATED = "LEARNING_UPDATED"
STAGE_AUDIT_COMMITTED = "AUDIT_COMMITTED"

ALL_LIFECYCLE_STAGES = [
    STAGE_EVENT_RECEIVED,
    STAGE_OPPORTUNITY_CREATED,
    STAGE_FAILURE_DIAGNOSED,
    STAGE_UNDERWRITING_COMPLETED,
    STAGE_STRATEGIES_EVALUATED,
    STAGE_ACTION_SELECTED,
    STAGE_POLICY_CHECK,
    STAGE_PROVIDER_STATE_VERIFIED,
    STAGE_EXECUTION_ATTEMPTED,
    STAGE_OUTCOME_OBSERVED,
    STAGE_LEARNING_UPDATED,
    STAGE_AUDIT_COMMITTED,
]


@dataclass
class AgentExecutionResult:
    event_id: str
    payment_id: str
    status: str
    failure_class: Optional[str] = None
    selected_strategy: Optional[str] = None
    policy_verdict: Optional[str] = None
    provider_disposition: Optional[str] = None
    provider_capability: Optional[str] = None
    execution_status: Optional[str] = None
    audit_seq: Optional[int] = None
    audit_hash: Optional[str] = None
    detail: str = ""
    error: Optional[str] = None
    lifecycle_trace: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AgentLifecycleLogger:
    """Emits structured, sanitized JSON logs for each lifecycle stage."""

    def __init__(self, event_id: str, payment_id: str, correlation_id: Optional[str] = None):
        self.event_id = event_id
        self.payment_id = payment_id
        self.correlation_id = correlation_id or f"corr_{uuid.uuid4().hex[:12]}"
        self._t0 = time.monotonic()
        self.trace: List[Dict[str, Any]] = []

    def emit(self, stage: str, **kwargs: Any) -> Dict[str, Any]:
        elapsed_ms = round((time.monotonic() - self._t0) * 1000, 2)
        sanitized = {
            k: v for k, v in kwargs.items()
            if k not in {"secret", "api_key", "password", "token", "signature", "key_secret"}
        }
        record = {
            "agent": "RecoverSense",
            "stage": stage,
            "event_id": self.event_id,
            "payment_id": self.payment_id,
            "correlation_id": self.correlation_id,
            "elapsed_ms": elapsed_ms,
            **sanitized,
        }
        self.trace.append(record)
        logger.info(json.dumps(record, default=str))
        return record

    def emit_error(self, stage: str, error: str, **kwargs: Any) -> Dict[str, Any]:
        elapsed_ms = round((time.monotonic() - self._t0) * 1000, 2)
        record = {
            "agent": "RecoverSense",
            "stage": stage,
            "status": "ERROR",
            "event_id": self.event_id,
            "payment_id": self.payment_id,
            "correlation_id": self.correlation_id,
            "elapsed_ms": elapsed_ms,
            "error": error,
            **kwargs,
        }
        self.trace.append(record)
        logger.error(json.dumps(record, default=str))
        return record


class AgentRuntime:
    """Production runtime that orchestrates the 12-stage RecoverSense agent lifecycle."""

    def __init__(
        self,
        default_execution_adapter: Optional[ExecutionAdapter] = None,
        default_recovery_model: Optional[RecoveryModel] = None,
    ):
        self._default_adapter = default_execution_adapter
        self._default_model = default_recovery_model

    def _get_model(self) -> RecoveryModel:
        if self._default_model is not None:
            return self._default_model
        from app.routers.decisions import _recovery_model
        return _recovery_model

    def _get_adapter(self, source: Optional[str], mode: Optional[str]) -> ExecutionAdapter:
        if self._default_adapter is not None:
            return self._default_adapter
        from app.routers.decisions import _execution_adapter
        return get_execution_adapter_for_event(source, mode, simulator_adapter=_execution_adapter)

    def _load_performance_map(self, db: Any) -> Dict[str, Any]:
        """Load learned strategy performance for closed-loop optimization.

        Pillar 4: future decisions must USE updated performance. Returns the
        learned per-(strategy, context) statistics recorded from observable
        outcomes; empty on cold start (neutral adjustment).
        """
        from learning.closed_loop import performance_map_from_db
        return performance_map_from_db(db)

    def _execute_yield_curve_decision(
        self,
        *,
        db: Any,
        event_row: Any,
        yield_curve_result: Any,
        recovery_probability: float,
        policy_decision: Any,
        duplicate_check: Any,
        provider_status: str,
        provider_disposition: str,
    ) -> ExecutionResult:
        """Deterministically authorize and record a Yield Curve partial recovery.

        AI/model output (the underwriting result) proposes the candidate; this
        deterministic code authorizes execution. Returns one of:
        DUPLICATE_BLOCKED, POLICY_BLOCKED, or INTERNAL_LEDGER_ONLY.

        This is an internal financial decision/ledger representation ONLY —
        the current Razorpay integration has no partial-amount charge write
        operation, so the authorized partial recovery is recorded internally
        with capability UNSUPPORTED_PROVIDER_OPERATION. No external API call
        is made and no external payment success is claimed.
        """
        yc_best = yield_curve_result.best_candidate

        # Idempotency: at most one Yield Curve decision per event.
        existing_yc = (
            db.query(YieldCurveDecisionDB)
            .filter(YieldCurveDecisionDB.event_id == event_row.event_id)
            .first()
        )
        if existing_yc is not None:
            return ExecutionResult(
                status="DUPLICATE_BLOCKED",
                adapter="yield_curve_ledger",
                idempotency_key=existing_yc.idempotency_key,
                detail=(
                    "Yield Curve decision already recorded for this event; "
                    "duplicate ignored (idempotency preserved)."
                ),
                capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
            )

        yc_ledger = create_yield_curve_ledger(
            event_id=event_row.event_id,
            payment_id=event_row.payment_id,
            customer_id=event_row.customer_id,
            original_amount=event_row.amount,
            partial_level=yc_best.level,
            partial_amount=yc_best.partial_amount,
            remaining_balance=yc_best.remaining_balance,
            base_recovery_probability=recovery_probability,
            partial_recovery_probability=yc_best.partial_recovery_probability,
            expected_utility=yc_best.expected_utility,
            idempotency_key=f"yc_{event_row.event_id}",
            scheduled_windows=yield_curve_result.projected_schedule,
        )
        # Deterministic ledger gate: re-verifies idempotency and the
        # autonomous amount ceiling before any authorization.
        yc_authorization = authorize_partial_recovery(
            yc_ledger,
            idempotency_valid=not duplicate_check.is_duplicate,
            autonomous_amount=(policy_decision.verdict == "APPROVE"),
        )
        if not yc_authorization.get("authorized"):
            return ExecutionResult(
                status="POLICY_BLOCKED",
                adapter="yield_curve_ledger",
                idempotency_key=f"yc_{event_row.event_id}",
                detail=(
                    f"Yield Curve authorization refused by ledger gate: "
                    f"{yc_authorization.get('reason')}."
                ),
                capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
            )

        # PARTIAL_RECOVERY_AUTHORIZED → PARTIAL_RECOVERY_RECORDED (internal
        # ledger entry) → REMAINING_BALANCE_SCHEDULED (future pay cycles).
        record_partial_recovery(yc_ledger)
        schedule_remaining_balance(yc_ledger, yield_curve_result.projected_schedule)
        db.add(
            YieldCurveDecisionDB(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                customer_id=event_row.customer_id,
                original_amount=event_row.amount,
                partial_level=yc_best.level,
                partial_amount=yc_best.partial_amount,
                remaining_balance=yc_best.remaining_balance,
                base_recovery_probability=recovery_probability,
                partial_recovery_probability=yc_best.partial_recovery_probability,
                expected_utility=yc_best.expected_utility,
                observed_partial_recovery=0.0,
                outstanding_balance=yc_best.remaining_balance,
                state=yc_ledger.state.value,
                idempotency_key=f"yc_{event_row.event_id}",
                underwriting_result_json=yield_curve_result.to_dict(),
                policy_checks_json=[c.__dict__ for c in policy_decision.checks],
                provider_verification_json={
                    "status": provider_status,
                    "disposition": provider_disposition,
                    "verified_at": datetime.utcnow().isoformat(),
                },
                scheduled_windows_json=yield_curve_result.projected_schedule,
                source=event_row.source,
                mode=event_row.mode,
                simulation_id=event_row.simulation_id,
            )
        )
        return ExecutionResult(
            status="INTERNAL_LEDGER_ONLY",
            adapter="yield_curve_ledger",
            idempotency_key=f"yc_{event_row.event_id}",
            detail=(
                f"Yield Curve partial recovery recorded internally. "
                f"Level: {yc_best.level}X. Partial amount: {yc_best.partial_amount}. "
                f"Remaining scheduled: {yc_best.remaining_balance}. No external "
                f"partial-charge write operation exists in the current Razorpay "
                f"integration; nothing was faked."
            ),
            capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
        )

    def _execute_multi_rail_decision(
        self,
        *,
        db: Any,
        event_row: Any,
        multi_rail_result: Any,
        recovery_probability: float,
        policy_decision: Any,
        duplicate_check: Any,
        provider_status: str,
        provider_disposition: str,
    ) -> ExecutionResult:
        """Deterministically authorize and record a Multi-Rail switch.

        AI/model output (the underwriting result) proposes the target rail; this
        deterministic code authorizes execution. Returns one of:
        DUPLICATE_BLOCKED, POLICY_BLOCKED, or INTERNAL_LEDGER_ONLY.

        This is an internal financial decision/ledger representation ONLY —
        the current Razorpay integration has no mandate-creation / rail-switch
        write operation, so the authorized switch is recorded internally with
        capability UNSUPPORTED_PROVIDER_OPERATION. No external API call is made
        and no external mandate change is claimed.
        """
        mr_best = multi_rail_result.best_candidate

        # Idempotency: at most one Multi-Rail decision per event.
        existing_mr = (
            db.query(MultiRailDecisionDB)
            .filter(MultiRailDecisionDB.event_id == event_row.event_id)
            .first()
        )
        if existing_mr is not None:
            return ExecutionResult(
                status="DUPLICATE_BLOCKED",
                adapter="multi_rail_ledger",
                idempotency_key=existing_mr.idempotency_key,
                detail=(
                    "Multi-Rail decision already recorded for this event; "
                    "duplicate ignored (idempotency preserved)."
                ),
                capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
            )

        mr_ledger = create_multi_rail_ledger(
            event_id=event_row.event_id,
            payment_id=event_row.payment_id,
            customer_id=event_row.customer_id,
            source_rail=mr_best.source_rail,
            target_rail=mr_best.target_rail,
            original_amount=event_row.amount,
            base_recovery_probability=mr_best.base_recovery_probability,
            target_recovery_probability=mr_best.target_recovery_probability,
            expected_utility=mr_best.expected_utility,
            idempotency_key=f"mr_{event_row.event_id}",
        )
        # Deterministic ledger gate: re-verifies idempotency and the
        # autonomous amount ceiling before any authorization.
        mr_authorization = authorize_rail_switch(
            mr_ledger,
            idempotency_valid=not duplicate_check.is_duplicate,
            autonomous_amount=(policy_decision.verdict == "APPROVE"),
        )
        if not mr_authorization.get("authorized"):
            return ExecutionResult(
                status="POLICY_BLOCKED",
                adapter="multi_rail_ledger",
                idempotency_key=f"mr_{event_row.event_id}",
                detail=(
                    f"Multi-Rail authorization refused by ledger gate: "
                    f"{mr_authorization.get('reason')}."
                ),
                capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
            )

        # RAIL_SWITCH_AUTHORIZED → RAIL_SWITCH_RECORDED (internal ledger
        # entry) → AWAITING_RAIL_OUTCOME (waiting for observable result).
        record_rail_switch(mr_ledger)
        await_rail_outcome(mr_ledger)
        db.add(
            MultiRailDecisionDB(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                customer_id=event_row.customer_id,
                source_rail=mr_best.source_rail,
                target_rail=mr_best.target_rail,
                original_amount=event_row.amount,
                base_recovery_probability=mr_best.base_recovery_probability,
                target_recovery_probability=mr_best.target_recovery_probability,
                expected_utility=mr_best.expected_utility,
                observed_recovery=0.0,
                outstanding_amount=event_row.amount,
                state=mr_ledger.state.value,
                idempotency_key=f"mr_{event_row.event_id}",
                underwriting_result_json=multi_rail_result.to_dict(),
                policy_checks_json=[c.__dict__ for c in policy_decision.checks],
                provider_verification_json={
                    "status": provider_status,
                    "disposition": provider_disposition,
                    "verified_at": datetime.utcnow().isoformat(),
                },
                source=event_row.source,
                mode=event_row.mode,
                simulation_id=event_row.simulation_id,
            )
        )
        return ExecutionResult(
            status="INTERNAL_LEDGER_ONLY",
            adapter="multi_rail_ledger",
            idempotency_key=f"mr_{event_row.event_id}",
            detail=(
                f"Multi-Rail switch recorded internally. "
                f"{mr_best.source_rail} -> {mr_best.target_rail}. "
                f"Expected utility: {mr_best.expected_utility}. "
                f"No mandate-creation / rail-switch write operation exists in "
                f"the current Razorpay integration; nothing was faked."
            ),
            capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
        )


    def process_event(
        self,
        event_id: str,
        *,
        db: Optional[Any] = None,
        execution_adapter: Optional[ExecutionAdapter] = None,
        recovery_model: Optional[RecoveryModel] = None,
        policy_config: Optional[PolicyConfig] = None,
        cost_assumptions: Optional[CostAssumptions] = None,
        correlation_id: Optional[str] = None,
    ) -> AgentExecutionResult:
        """Run the full autonomous recovery lifecycle for a given event_id."""
        own_db = False
        if db is None:
            from app.database import SessionLocal
            db = SessionLocal()
            own_db = True

        lifecycle: Optional[AgentLifecycleLogger] = None
        try:
            event_row = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == event_id).first()
            if not event_row:
                logger.warning(json.dumps({
                    "agent": "RecoverSense",
                    "stage": STAGE_EVENT_RECEIVED,
                    "status": "NOT_FOUND",
                    "event_id": event_id,
                }))
                return AgentExecutionResult(
                    event_id=event_id,
                    payment_id="UNKNOWN",
                    status="ERROR",
                    detail=f"Payment event '{event_id}' not found in database.",
                    error="event_not_found",
                )

            lifecycle = AgentLifecycleLogger(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                correlation_id=correlation_id,
            )

            # STAGE 1: EVENT_RECEIVED
            lifecycle.emit(
                STAGE_EVENT_RECEIVED,
                status="OK",
                amount=event_row.amount,
                currency=event_row.currency,
                source=event_row.source,
                mode=event_row.mode,
                attempt_number=event_row.attempt_number,
                failure_reason=event_row.failure_reason,
            )

            # Idempotency check: Has this event already been processed?
            existing_decision = db.query(DecisionDB).filter(DecisionDB.event_id == event_id).first()
            if existing_decision:
                lifecycle.emit(
                    STAGE_EVENT_RECEIVED,
                    status="ALREADY_PROCESSED",
                    existing_action=existing_decision.action,
                    audit_seq=existing_decision.audit_seq,
                )
                return AgentExecutionResult(
                    event_id=event_id,
                    payment_id=event_row.payment_id,
                    status="ALREADY_PROCESSED",
                    failure_class=existing_decision.failure_class,
                    selected_strategy=existing_decision.selected_strategy,
                    policy_verdict=existing_decision.policy_verdict,
                    execution_status=existing_decision.action,
                    audit_seq=existing_decision.audit_seq,
                    audit_hash=existing_decision.audit_hash,
                    detail=f"Event {event_id} was already processed with action '{existing_decision.action}'.",
                    lifecycle_trace=lifecycle.trace,
                )

            # STAGE 2: OPPORTUNITY_CREATED
            event_dict = {
                "event_id": event_row.event_id,
                "customer_id": event_row.customer_id,
                "payment_id": event_row.payment_id,
                "subscription_id": event_row.subscription_id,
                "amount": event_row.amount,
                "currency": event_row.currency,
                "timestamp": event_row.timestamp,
                "failure_reason": event_row.failure_reason,
                "failure_class": event_row.failure_class,
                "attempt_number": event_row.attempt_number,
                "mandate_status": event_row.mandate_status,
                "payment_status": event_row.payment_status,
                "source": event_row.source,
                "mode": event_row.mode,
                "simulation_id": event_row.simulation_id,
                "metadata": event_row.metadata_json or {},
            }
            opportunity_id = f"opp_{event_row.event_id}"
            lifecycle.emit(
                STAGE_OPPORTUNITY_CREATED,
                status="OK",
                opportunity_id=opportunity_id,
                customer_id=event_row.customer_id,
                amount_at_risk=event_row.amount,
            )

            # STAGE 3: FAILURE_DIAGNOSED
            failure_class = event_row.failure_class or classify_failure(event_row.failure_reason)
            lifecycle.emit(
                STAGE_FAILURE_DIAGNOSED,
                status="OK",
                failure_reason=event_row.failure_reason,
                failure_class=failure_class,
            )

            # STAGE 4: UNDERWRITING_COMPLETED
            model = recovery_model or self._get_model()
            costs = cost_assumptions or CostAssumptions()
            policy_cfg = policy_config or PolicyConfig()

            history = event_dict["metadata"].get("history", [])
            timing = compute_timing_score(history)
            support_note = event_dict["metadata"].get("support_note", "")
            ai_context = get_context_recommendation(
                support_note=support_note,
                failure_class=failure_class,
                timing_window=timing.to_dict()["recommended_window"],
            )
            recovery_pred = model.predict(
                timing_score=timing.liquidity_timing_score,
                scheduled_hour=timing.circular_mean_hour,
                circular_mean_hour=timing.circular_mean_hour,
                n_history_points=timing.n_history_points,
                failure_class=failure_class,
                attempt_number=event_row.attempt_number,
                high_intent_signal=(ai_context.customer_intent == "HIGH"),
                failure_reason=event_row.failure_reason,
            )
            ev = compute_expected_net_recovery(
                amount=event_row.amount,
                recovery_probability=recovery_pred.probability,
                costs=costs,
            )
            lifecycle.emit(
                STAGE_UNDERWRITING_COMPLETED,
                status="OK",
                timing_score=round(timing.liquidity_timing_score, 4),
                recommended_window=timing.to_dict()["recommended_window"],
                recovery_probability=round(recovery_pred.probability, 4),
                expected_net_recovery=round(ev.expected_net_recovery, 2),
                customer_intent=ai_context.customer_intent,
            )

            # FLASH-SETTLE UNDERWRITING INTEGRATION
            # Invoke the existing Flash-Settle underwriting engine for eligible SOFT failures.
            # This is the integration point where Flash-Settle enters the Agent Runtime.
            flash_settle_result = None
            flash_settle_utility = 0.0
            flash_settle_eligible = False
            if failure_class == "SOFT":
                # Check for existing active exposure for this customer
                existing_flash_exposure = 0.0
                active_flash_states = {"AUTHORIZED", "ADVANCE_RECORDED", "AWAITING_RECOVERY"}
                existing_flash = (
                    db.query(FlashSettleExposureDB)
                    .filter(FlashSettleExposureDB.customer_id == event_row.customer_id)
                    .filter(FlashSettleExposureDB.state.in_(tuple(active_flash_states)))
                    .first()
                )
                if existing_flash:
                    existing_flash_exposure = existing_flash.outstanding_amount

                # Build underwriting inputs from the event data
                merchant_monthly_gmv = event_dict.get("metadata", {}).get("merchant_monthly_gmv", 10000.0)
                merchant_reserve_cap = event_dict.get("metadata", {}).get("merchant_reserve_cap", 0.0)

                uw_inputs = UnderwritingInputs(
                    event_id=event_row.event_id,
                    payment_id=event_row.payment_id,
                    customer_id=event_row.customer_id,
                    amount=event_row.amount,
                    failure_class=failure_class,
                    failure_reason=event_row.failure_reason,
                    recovery_probability=recovery_pred.probability,
                    recovery_horizon_hours=72.0,  # Default horizon for evaluation
                    merchant_id="default",
                    merchant_monthly_gmv=merchant_monthly_gmv,
                    merchant_reserve_cap=merchant_reserve_cap,
                    existing_active_exposure=existing_flash_exposure,
                    payment_state=event_row.payment_status or "FAILED",
                    idempotency_valid=True,
                    policy_restrictions=[],
                    metadata={"source": "agent_runtime"},
                )
                flash_settle_result = flash_settle_underwrite(uw_inputs)
                flash_settle_eligible = flash_settle_result.eligible

                # Compute Flash-Settle expected utility
                # Utility = expected recovery from advance - opportunity cost
                if flash_settle_eligible:
                    flash_settle_utility = (
                        flash_settle_result.proposed_advance_amount * recovery_pred.probability
                    )

            # YIELD CURVE UNDERWRITING INTEGRATION (Pillar 2)
            # When the full all-or-nothing recovery probability is below
            # beta_min, evaluate a partial-recovery decomposition (0.25X /
            # 0.50X / 0.75X) scored by net expected value:
            #     partial_amount x P(recovery | partial, timing) - charge cost
            # The AI/model proposes the best candidate; the deterministic
            # policy gate authorizes any execution (see STAGE 8/9 below).
            yield_curve_result = None
            yield_curve_utility = 0.0
            yield_curve_triggered = False
            if failure_class == "SOFT":
                yc_recommended_window = timing.to_dict()["recommended_window"]
                yc_inputs = YieldCurveInputs(
                    event_id=event_row.event_id,
                    payment_id=event_row.payment_id,
                    customer_id=event_row.customer_id,
                    amount=event_row.amount,
                    failure_class=failure_class,
                    failure_reason=event_row.failure_reason,
                    recovery_probability=recovery_pred.probability,
                    timing_score=timing.liquidity_timing_score,
                    circular_mean_hour=timing.circular_mean_hour,
                    scheduled_hour=timing.recommended_window_start_hour,
                    n_history_points=timing.n_history_points,
                    attempt_number=event_row.attempt_number,
                    high_intent_signal=(ai_context.customer_intent == "HIGH"),
                    cost_assumptions=costs,
                    timing_window=yc_recommended_window,
                    recommended_window=yc_recommended_window,
                )
                yield_curve_result = evaluate_yield_curve_underwriting(yc_inputs)
                yield_curve_triggered = (
                    yield_curve_result.triggered
                    and yield_curve_result.best_candidate is not None
                )
                if yield_curve_triggered:
                    yield_curve_utility = (
                        yield_curve_result.best_candidate.expected_utility
                    )

            # MULTI-RAIL UNDERWRITING INTEGRATION (Pillar 3)
            # When the current rail is a supported source, evaluate whether
            # switching to the next rail in the chain yields better expected
            # recovery economics than retrying the current rail. The AI/model
            # proposes the target rail; the deterministic policy gate authorizes
            # any execution (see STAGE 8/9 below).
            multi_rail_result = None
            multi_rail_utility = 0.0
            multi_rail_triggered = False
            if failure_class == "SOFT":
                current_rail = event_dict.get("metadata", {}).get("current_rail")
                mr_inputs = MultiRailInputs(
                    event_id=event_row.event_id,
                    payment_id=event_row.payment_id,
                    customer_id=event_row.customer_id,
                    amount=event_row.amount,
                    failure_class=failure_class,
                    failure_reason=event_row.failure_reason,
                    recovery_probability=recovery_pred.probability,
                    current_rail=current_rail,
                    timing_score=timing.liquidity_timing_score,
                    attempt_number=event_row.attempt_number,
                    cost_assumptions=costs,
                )
                multi_rail_result = evaluate_multi_rail_engine(mr_inputs)
                multi_rail_triggered = (
                    multi_rail_result.triggered
                    and multi_rail_result.best_candidate is not None
                )
                if multi_rail_triggered:
                    multi_rail_utility = (
                        multi_rail_result.best_candidate.expected_utility
                    )

            # STAGE 5: STRATEGIES_EVALUATED
            analysis_item = {
                "event": event_dict,
                "failure_class": failure_class,
                "timing_score": timing.liquidity_timing_score,
                "timing_window": timing.to_dict()["recommended_window"],
                "recovery_probability": recovery_pred.probability,
            }
            opportunities = optimize_portfolio(
                [analysis_item],
                max_actions_per_cycle=1,
                costs=costs,
                policy_config=policy_cfg,
                performance=self._load_performance_map(db),
            )
            opp = opportunities[0] if opportunities else None
            candidates_summary = [
                {"strategy": c["strategy"], "eligible": c["eligible"], "expected_utility": c.get("expected_utility", 0.0)}
                for c in (opp.alternatives if opp else [])
            ]

            # Include Flash-Settle in the candidates summary if it was evaluated
            if flash_settle_result is not None:
                candidates_summary.append({
                    "strategy": "FLASH_SETTLE",
                    "eligible": flash_settle_eligible,
                    "expected_utility": round(flash_settle_utility, 2),
                })

            # Include Yield Curve in the candidates summary if it was evaluated.
            # Existing strategy competition is preserved: Yield Curve is only
            # marked eligible when its full-recovery probability trigger fired.
            if yield_curve_result is not None:
                candidates_summary.append({
                    "strategy": "YIELD_CURVE",
                    "eligible": yield_curve_triggered,
                    "expected_utility": round(yield_curve_utility, 2),
                })

            # Include Multi-Rail in the candidates summary if it was evaluated.
            # Existing strategy competition is preserved: Multi-Rail is only
            # marked eligible when the current rail is a supported source.
            if multi_rail_result is not None:
                candidates_summary.append({
                    "strategy": "MULTI_RAIL",
                    "eligible": multi_rail_triggered,
                    "expected_utility": round(multi_rail_utility, 2),
                })

            lifecycle.emit(
                STAGE_STRATEGIES_EVALUATED,
                status="OK",
                n_candidates=len(candidates_summary),
                candidates=candidates_summary,
                flash_settle_evaluated=flash_settle_result is not None,
                flash_settle_eligible=flash_settle_eligible,
                yield_curve_evaluated=yield_curve_result is not None,
                yield_curve_triggered=yield_curve_triggered,
                multi_rail_evaluated=multi_rail_result is not None,
                multi_rail_triggered=multi_rail_triggered,
            )

            # STAGE 6: ACTION_SELECTED
            # Select Flash-Settle if it's eligible and has higher expected utility
            selected_strategy = opp.selected_strategy if opp else "STOP"
            expected_utility = opp.expected_utility if opp else 0.0
            expected_recovery = opp.expected_recovery if opp else 0.0
            policy_recommendation = opp.policy_recommendation if opp else "NO_ACTION"

            # Flash-Settle selection: only if eligible AND has higher utility than optimizer's choice
            if flash_settle_eligible and flash_settle_utility > expected_utility:
                selected_strategy = "FLASH_SETTLE"
                expected_utility = flash_settle_utility
                expected_recovery = (
                    flash_settle_result.proposed_advance_amount * recovery_pred.probability
                )
                policy_recommendation = "FLASH_SETTLE"

            # Yield Curve selection: only when triggered AND its net expected
            # utility exceeds the current best (optimizer's choice or
            # Flash-Settle). Highest expected economic utility wins.
            if yield_curve_triggered and yield_curve_utility > expected_utility:
                selected_strategy = "YIELD_CURVE"
                expected_utility = yield_curve_utility
                expected_recovery = (
                    yield_curve_result.best_candidate.gross_expected_recovery
                )
                policy_recommendation = "PARTIAL_RECOVERY"

            # Multi-Rail selection: only when triggered AND its net expected
            # utility exceeds the current best (optimizer, Flash-Settle, or
            # Yield Curve). Highest expected economic utility wins.
            if multi_rail_triggered and multi_rail_utility > expected_utility:
                selected_strategy = "MULTI_RAIL"
                expected_utility = multi_rail_utility
                expected_recovery = (
                    multi_rail_result.best_candidate.gross_expected_recovery
                )
                policy_recommendation = "RAIL_SWITCH"
            lifecycle.emit(
                STAGE_ACTION_SELECTED,
                status="OK",
                selected_strategy=selected_strategy,
                expected_utility=round(expected_utility, 2),
                expected_recovery=round(expected_recovery, 2),
                policy_recommendation=policy_recommendation,
            )

            # STAGE 7: POLICY_CHECK
            duplicate_check = is_duplicate_action(
                db,
                payment_id=event_row.payment_id,
                attempt_number=event_row.attempt_number,
                recommended_action=ai_context.recommended_action,
                source=event_row.source,
                mode=event_row.mode,
                dedupe_window_minutes=policy_cfg.dedupe_window_minutes,
            )
            policy_decision = evaluate_policy(
                recommended_action=ai_context.recommended_action,
                attempt_number=event_row.attempt_number,
                failure_class=failure_class,
                amount=event_row.amount,
                expected_net_recovery=ev.expected_net_recovery,
                recommended_window=ai_context.recommended_window,
                already_actioned_recently=duplicate_check.is_duplicate,
                config=policy_cfg,
            )
            lifecycle.emit(
                STAGE_POLICY_CHECK,
                status="OK",
                verdict=policy_decision.verdict,
                passed=all(c.passed for c in policy_decision.checks),
                failed_checks=[c.name for c in policy_decision.checks if not c.passed],
                effective_window=policy_decision.effective_window,
                original_window=policy_decision.original_window,
                duplicate_blocked=duplicate_check.is_duplicate,
            )

            # STAGE 8: PROVIDER_STATE_VERIFIED
            adapter = execution_adapter or self._get_adapter(event_row.source, event_row.mode)
            is_simulation = (event_row.source or "").upper() in {"SIMULATOR", "SIMULATION"} or (event_row.mode or "").upper() == "SIMULATION"
            execution_payment_id = event_row.event_id if is_simulation else event_row.payment_id
            idempotency_key = (
                f"{event_row.event_id}:{event_row.attempt_number}"
                if is_simulation else f"{event_row.payment_id}:{event_row.attempt_number}"
            )

            execution_result: Optional[ExecutionResult] = None
            final_action = policy_decision.verdict
            provider_status = "NOT_CHECKED"
            provider_disposition = "N/A"

            # FLASH-SETTLE EXECUTION: Internal ledger only, no external transfer
            # If Flash-Settle is selected, record the internal advance without
            # calling any external Razorpay payout/transfer endpoint.
            if selected_strategy == "FLASH_SETTLE" and flash_settle_result is not None:
                # Verify provider state first (fresh provider state verification)
                current_state = adapter.get_payment_state(execution_payment_id)
                state_safety = normalize_payment_state(current_state)
                provider_status = state_safety.normalized_status
                provider_disposition = state_safety.disposition

                # Block Flash-Settle if payment is already captured/successful
                if state_safety.disposition == "NEVER_RETRY":
                    execution_result = ExecutionResult(
                        status="SKIPPED_STALE_STATE",
                        adapter="flash_settle_ledger",
                        idempotency_key=f"fs_{event_row.event_id}",
                        detail=f"Flash-Settle blocked: payment already in terminal state. {state_safety.detail}",
                        capability=ProviderCapability.SIMULATED_OPERATION.value,
                    )
                    final_action = execution_result.status
                    # Revert to optimizer's choice since Flash-Settle is blocked
                    selected_strategy = opp.selected_strategy if opp else "STOP"
                    expected_utility = opp.expected_utility if opp else 0.0
                else:
                    # Record the internal advance (INTERNAL_LEDGER_ONLY)
                    # This does NOT call any external Razorpay payout/transfer endpoint
                    exposure_db = FlashSettleExposureDB(
                        event_id=event_row.event_id,
                        payment_id=event_row.payment_id,
                        customer_id=event_row.customer_id,
                        merchant_id="default",
                        recovery_probability=flash_settle_result.recovery_probability,
                        recovery_horizon_hours=flash_settle_result.recovery_horizon_hours,
                        proposed_advance_amount=flash_settle_result.proposed_advance_amount,
                        merchant_exposure=flash_settle_result.merchant_exposure,
                        merchant_reserve_cap=flash_settle_result.merchant_reserve_cap,
                        advance_amount=flash_settle_result.proposed_advance_amount,
                        recovered_amount=0.0,
                        outstanding_amount=flash_settle_result.proposed_advance_amount,
                        state="AWAITING_RECOVERY",
                        idempotency_key=f"fs_{event_row.event_id}",
                        underwriting_result_json=flash_settle_result.to_dict(),
                        policy_checks_json=[c.__dict__ for c in policy_decision.checks],
                        provider_verification_json={
                            "status": provider_status,
                            "disposition": provider_disposition,
                            "verified_at": datetime.utcnow().isoformat(),
                        },
                        source=event_row.source,
                        mode=event_row.mode,
                        simulation_id=event_row.simulation_id,
                    )
                    db.add(exposure_db)

                    execution_result = ExecutionResult(
                        status="INTERNAL_LEDGER_ONLY",
                        adapter="flash_settle_ledger",
                        idempotency_key=f"fs_{event_row.event_id}",
                        detail=(
                            f"Flash-Settle advance recorded internally. "
                            f"Amount: {flash_settle_result.proposed_advance_amount}. "
                            f"No external transfer occurred. "
                            f"State: AWAITING_RECOVERY."
                        ),
                        capability=ProviderCapability.SIMULATED_OPERATION.value,
                    )
                    final_action = "INTERNAL_LEDGER_ONLY"

            elif selected_strategy == "YIELD_CURVE" and yield_curve_result is not None:
                # YIELD CURVE EXECUTION (Pillar 2). Fresh provider state is
                # verified first; the deterministic policy gate authorizes
                # inside _execute_yield_curve_decision. No partial-amount
                # charge write exists in the current Razorpay integration,
                # so an authorized partial recovery is recorded internally
                # (INTERNAL_LEDGER_ONLY / UNSUPPORTED_PROVIDER_OPERATION) —
                # never faked as an external payment.
                current_state = adapter.get_payment_state(execution_payment_id)
                state_safety = normalize_payment_state(current_state)
                provider_status = state_safety.normalized_status
                provider_disposition = state_safety.disposition

                if state_safety.disposition == "NEVER_RETRY":
                    # Fresh provider-state verification blocks execution:
                    # the payment is already captured/successful/settled.
                    execution_result = ExecutionResult(
                        status="SKIPPED_STALE_STATE",
                        adapter="yield_curve_ledger",
                        idempotency_key=f"yc_{event_row.event_id}",
                        detail=(
                            f"Yield Curve blocked: payment already in terminal "
                            f"state. {state_safety.detail}"
                        ),
                        capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
                    )
                    final_action = execution_result.status
                    # Revert to the optimizer's choice: Yield Curve is blocked.
                    selected_strategy = opp.selected_strategy if opp else "STOP"
                    expected_utility = opp.expected_utility if opp else 0.0
                else:
                    execution_result = self._execute_yield_curve_decision(
                        db=db,
                        event_row=event_row,
                        yield_curve_result=yield_curve_result,
                        recovery_probability=recovery_pred.probability,
                        policy_decision=policy_decision,
                        duplicate_check=duplicate_check,
                        provider_status=provider_status,
                        provider_disposition=provider_disposition,
                    )
                    final_action = execution_result.status

            elif selected_strategy == "MULTI_RAIL" and multi_rail_result is not None:
                # MULTI-RAIL EXECUTION (Pillar 3). Fresh provider state is
                # verified first; the deterministic policy gate authorizes
                # inside _execute_multi_rail_decision. No mandate-creation /
                # rail-switch write operation exists in the current Razorpay
                # integration, so an authorized rail switch is recorded
                # internally (INTERNAL_LEDGER_ONLY /
                # UNSUPPORTED_PROVIDER_OPERATION) — never faked as an external
                # mandate change.
                mr_best = multi_rail_result.best_candidate
                current_state = adapter.get_payment_state(execution_payment_id)
                state_safety = normalize_payment_state(current_state)
                provider_status = state_safety.normalized_status
                provider_disposition = state_safety.disposition

                if state_safety.disposition == "NEVER_RETRY":
                    # Fresh provider-state verification blocks execution:
                    # the payment is already captured/successful/settled.
                    execution_result = ExecutionResult(
                        status="SKIPPED_STALE_STATE",
                        adapter="multi_rail_ledger",
                        idempotency_key=f"mr_{event_row.event_id}",
                        detail=(
                            f"Multi-Rail blocked: payment already in terminal "
                            f"state. {state_safety.detail}"
                        ),
                        capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
                    )
                    final_action = execution_result.status
                    # Revert to the optimizer's choice: Multi-Rail is blocked.
                    selected_strategy = opp.selected_strategy if opp else "STOP"
                    expected_utility = opp.expected_utility if opp else 0.0
                else:
                    execution_result = self._execute_multi_rail_decision(
                        db=db,
                        event_row=event_row,
                        multi_rail_result=multi_rail_result,
                        recovery_probability=recovery_pred.probability,
                        policy_decision=policy_decision,
                        duplicate_check=duplicate_check,
                        provider_status=provider_status,
                        provider_disposition=provider_disposition,
                    )
                    final_action = execution_result.status

            elif selected_strategy == "VOICE_RECOVERY":
                # VOICE RECOVERY (bounded simulator channel).
                # Deterministic policy authorization is checked FIRST: the AI
                # context recommendation never authorizes anything, so a
                # non-APPROVE policy verdict blocks voice before any execution
                # attempt (the provider is not even queried). The Safety Gate
                # below remains the independent second line of defense.
                # Voice is a SIMULATED_OPERATION — no real telephony call is
                # placed. It never bypasses policy, never modifies amounts,
                # and never authorizes payments.
                voice_adapter = get_default_voice_adapter()
                provider_write_capability = adapter.check_capability("schedule_retry")
                # A caller that explicitly supplies Razorpay's Test Mode
                # read-only adapter has no executable provider write path.
                # Never substitute a Voice simulation and report it as that
                # provider's execution: preserve the adapter's unsupported
                # capability honestly.
                if (
                    getattr(adapter, "name", None) == "razorpay_test_mode"
                    and provider_write_capability == ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION
                ):
                    execution_result = ExecutionResult(
                        status="UNSUPPORTED_PROVIDER_OPERATION",
                        adapter=getattr(adapter, "name", "unknown"),
                        idempotency_key=idempotency_key,
                        detail=(
                            "No documented Razorpay Test Mode endpoint supports this write; "
                            "the configured adapter is read-only, "
                            "no provider write or substituted simulation was performed."
                        ),
                        capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
                    )
                    final_action = execution_result.status
                elif policy_decision.verdict != "APPROVE":
                    execution_result = ExecutionResult(
                        status="POLICY_BLOCKED",
                        adapter="voice_simulator",
                        idempotency_key=idempotency_key,
                        detail=(
                            f"Voice recovery blocked by deterministic policy "
                            f"verdict '{policy_decision.verdict}': "
                            f"{policy_decision.reason_summary}"
                        ),
                        capability=ProviderCapability.SIMULATED_OPERATION.value,
                    )
                    final_action = execution_result.status
                else:
                    current_state = adapter.get_payment_state(execution_payment_id)
                    state_safety = normalize_payment_state(current_state)
                    provider_status = state_safety.normalized_status
                    provider_disposition = state_safety.disposition

                    if state_safety.disposition == "NEVER_RETRY":
                        execution_result = ExecutionResult(
                            status="SKIPPED_STALE_STATE",
                            adapter="voice_simulator",
                            idempotency_key=idempotency_key,
                            detail=f"Voice blocked: payment already in terminal state. {state_safety.detail}",
                            capability=ProviderCapability.SIMULATED_OPERATION.value,
                        )
                        final_action = execution_result.status
                    elif state_safety.disposition == "DEFER":
                        execution_result = ExecutionResult(
                            status="DEFERRED_PROVIDER_STATE",
                            adapter="voice_simulator",
                            idempotency_key=idempotency_key,
                            detail=f"Voice deferred safely — {state_safety.detail}",
                            raw_response=current_state,
                            capability=ProviderCapability.SIMULATED_OPERATION.value,
                        )
                        final_action = execution_result.status
                    else:
                        # Pillar 5 — deterministic Safety Gate: voice strategy
                        # must still pass the gate before any simulated execution.
                        safety_gate_decision = evaluate_safety_gate(SafetyGateContext(
                            event_id=event_row.event_id,
                            payment_id=event_row.payment_id,
                            attempt_number=event_row.attempt_number,
                            requested_amount=event_row.amount,
                            strategy="VOICE_RECOVERY",
                            expected_net_recovery=ev.expected_net_recovery,
                            failure_class=failure_class,
                            provider_status=provider_status,
                            provider_disposition=provider_disposition,
                            provider_verified=True,
                            idempotency_duplicate=duplicate_check.is_duplicate,
                            config=policy_cfg,
                        ))
                        if not safety_gate_decision.approved:
                            execution_result = ExecutionResult(
                                status=(
                                    "SAFETY_GATE_BLOCKED"
                                    if safety_gate_decision.verdict == "BLOCK"
                                    else "SAFETY_GATE_DEFERRED"
                                ),
                                adapter="voice_simulator",
                                idempotency_key=idempotency_key,
                                detail=safety_gate_decision.reason,
                                capability=ProviderCapability.SAFE_DEFER.value,
                                safety_gate=safety_gate_decision.to_dict(),
                            )
                            final_action = execution_result.status
                        else:
                            voice_window = policy_decision.effective_window or ai_context.recommended_window
                            voice_window_start = voice_window.split("-")[0] if voice_window else "18:00"
                            execution_result = voice_adapter.schedule_voice_call(
                                payment_id=execution_payment_id,
                                customer_id=event_row.customer_id,
                                window_start_iso=f"{str(event_row.timestamp)[:10]}T{voice_window_start}:00",
                                idempotency_key=idempotency_key,
                            )
                            execution_result.safety_gate = safety_gate_decision.to_dict()
                            final_action = execution_result.status

            elif policy_decision.verdict == "APPROVE" and ai_context.recommended_action == "SCHEDULE_RETRY":
                current_state = adapter.get_payment_state(execution_payment_id)
                state_safety = normalize_payment_state(current_state)
                provider_status = state_safety.normalized_status
                provider_disposition = state_safety.disposition

                if state_safety.disposition == "NEVER_RETRY":
                    execution_result = ExecutionResult(
                        status="SKIPPED_STALE_STATE",
                        adapter=getattr(adapter, "name", "unknown"),
                        idempotency_key=idempotency_key,
                        detail=f"Skipped execution — {state_safety.detail} (normalized: '{state_safety.normalized_status}').",
                        capability=getattr(adapter, "check_capability", lambda op: ProviderCapability.SIMULATED_OPERATION)("schedule_retry").value,
                    )
                    final_action = execution_result.status
                elif state_safety.disposition == "DEFER":
                    execution_result = ExecutionResult(
                        status="DEFERRED_PROVIDER_STATE",
                        adapter=getattr(adapter, "name", "unknown"),
                        idempotency_key=idempotency_key,
                        detail=f"Deferred execution safely — {state_safety.detail} (normalized: '{state_safety.normalized_status}').",
                        raw_response=current_state,
                        capability=getattr(adapter, "check_capability", lambda op: ProviderCapability.SIMULATED_OPERATION)("schedule_retry").value,
                    )
                    final_action = execution_result.status
                else:
                    capability = getattr(adapter, "check_capability", lambda op: ProviderCapability.SIMULATED_OPERATION)("schedule_retry")
                    if capability == ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION:
                        execution_result = ExecutionResult(
                            status="UNSUPPORTED_PROVIDER_OPERATION",
                            adapter=getattr(adapter, "name", "unknown"),
                            idempotency_key=idempotency_key,
                            detail="No documented Razorpay Test Mode endpoint for arbitrary scheduled UPI mandate retry was used. Action bounded safely; not faked.",
                            capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
                        )
                        final_action = execution_result.status
                    elif capability == ProviderCapability.SAFE_DEFER:
                        execution_result = ExecutionResult(
                            status="DEFERRED_PROVIDER_STATE",
                            adapter=getattr(adapter, "name", "unknown"),
                            idempotency_key=idempotency_key,
                            detail="Provider capability check requires deferral.",
                            capability=ProviderCapability.SAFE_DEFER.value,
                        )
                        final_action = execution_result.status
                    else:
                        effective_exec_window = policy_decision.effective_window or ai_context.recommended_window
                        window_start = effective_exec_window.split("-")[0] if effective_exec_window else "18:00"
                        # Pillar 5 — deterministic Safety Gate: strategy selected,
                        # policy authorized, fresh provider state verified. The gate
                        # is the final defend-line before any external execution.
                        # If it blocks/defers, execution MUST NOT happen.
                        safety_gate_decision = evaluate_safety_gate(SafetyGateContext(
                            event_id=event_row.event_id,
                            payment_id=event_row.payment_id,
                            attempt_number=event_row.attempt_number,
                            requested_amount=event_row.amount,
                            strategy=selected_strategy or policy_recommendation,
                            expected_net_recovery=ev.expected_net_recovery,
                            failure_class=failure_class,
                            provider_status=provider_status,
                            provider_disposition=provider_disposition,
                            provider_verified=True,
                            idempotency_duplicate=duplicate_check.is_duplicate,
                            config=policy_cfg,
                        ))
                        if not safety_gate_decision.approved:
                            execution_result = ExecutionResult(
                                status=(
                                    "SAFETY_GATE_BLOCKED"
                                    if safety_gate_decision.verdict == "BLOCK"
                                    else "SAFETY_GATE_DEFERRED"
                                ),
                                adapter="safety_gate",
                                idempotency_key=idempotency_key,
                                detail=safety_gate_decision.reason,
                                capability=ProviderCapability.SAFE_DEFER.value,
                                safety_gate=safety_gate_decision.to_dict(),
                            )
                            final_action = execution_result.status
                        else:
                            # REAL RAZORPAY UPI AUTOPAY S2S SUBSEQUENT-DEBIT
                            # (documented flow, strictly opt-in). Only reached
                            # AFTER policy authorization + fresh provider state
                            # verification + Safety Gate approval. Never used
                            # for simulation-mode events. When real execution
                            # is disabled (default) or the event has no UPI
                            # Autopay mandate context, this returns None and
                            # the existing simulator path runs unchanged.
                            real_execution_result = (
                                attempt_real_upi_autopay_execution(
                                    metadata=event_dict.get("metadata"),
                                    amount_rupees=event_row.amount,
                                    payment_id=execution_payment_id,
                                    idempotency_key=idempotency_key,
                                    fresh_provider_state=current_state,
                                )
                                if not is_simulation else None
                            )
                            if real_execution_result is not None:
                                execution_result = real_execution_result
                            else:
                                execution_result = adapter.schedule_retry(
                                    payment_id=execution_payment_id,
                                    window_start_iso=f"{str(event_row.timestamp)[:10]}T{window_start}:00",
                                    idempotency_key=idempotency_key,
                                )
                            # Attach the gate decision so it is audited even on approval.
                            execution_result.safety_gate = safety_gate_decision.to_dict()
                            final_action = execution_result.status

            lifecycle.emit(
                STAGE_PROVIDER_STATE_VERIFIED,
                status="OK",
                provider_status=provider_status,
                disposition=provider_disposition,
            )

            # STAGE 9: EXECUTION_ATTEMPTED
            lifecycle.emit(
                STAGE_EXECUTION_ATTEMPTED,
                status="OK",
                execution_status=(execution_result.status if execution_result else "NOT_ATTEMPTED"),
                adapter=(execution_result.adapter if execution_result else "none"),
                capability=(execution_result.capability if execution_result else "none"),
                idempotency_key=(execution_result.idempotency_key if execution_result else None),
            )

            # STAGE 10: OUTCOME_OBSERVED
            # Note: Flash-Settle recovery observation happens when the payment outcome
            # is recorded via the record_outcome endpoint. The internal ledger is
            # updated at that time.
            lifecycle.emit(
                STAGE_OUTCOME_OBSERVED,
                status="OK",
                final_action=final_action,
                execution_status=(execution_result.status if execution_result else "NOT_EXECUTED"),
                recovery_probability=round(recovery_pred.probability, 4),
                expected_net_recovery=round(ev.expected_net_recovery, 2),
                flash_settle_used=(selected_strategy == "FLASH_SETTLE"),
                flash_settle_state=(
                    "AWAITING_RECOVERY" if selected_strategy == "FLASH_SETTLE" and final_action == "INTERNAL_LEDGER_ONLY"
                    else None
                ),
                yield_curve_used=(selected_strategy == "YIELD_CURVE"),
                yield_curve_state=(
                    "REMAINING_BALANCE_SCHEDULED" if selected_strategy == "YIELD_CURVE" and final_action == "INTERNAL_LEDGER_ONLY"
                    else None
                ),
                multi_rail_used=(selected_strategy == "MULTI_RAIL"),
                multi_rail_state=(
                    "AWAITING_RAIL_OUTCOME" if selected_strategy == "MULTI_RAIL" and final_action == "INTERNAL_LEDGER_ONLY"
                    else None
                ),
                voice_used=(selected_strategy == "VOICE_RECOVERY"),
                voice_state=(
                    "VOICE_SIMULATED" if selected_strategy == "VOICE_RECOVERY" and final_action == "SCHEDULED"
                    else None
                ),
            )

            # STAGE 11: LEARNING_UPDATED
            # Pillar 4 — closed-loop learning: predicted utilities for every
            # candidate are retained (alternatives_json) for counterfactual
            # labelling; REALIZED utility is computed only from observable
            # outcomes via POST /outcomes/{event_id}, which updates the
            # strategy-performance statistics consumed above.
            lifecycle.emit(
                STAGE_LEARNING_UPDATED,
                status="OK",
                strategy=selected_strategy,
                expected_recovery=round(expected_recovery, 2),
                learning_active=True,
                performance_used=bool(self._load_performance_map(db)),
                counterfactuals_retained=len(candidates_summary),
                note=(
                    "Closed-loop learning active: candidate utilities retained "
                    "as ESTIMATED_COUNTERFACTUAL until an observable outcome "
                    "yields realized utility. Learning never overrides the "
                    "deterministic policy gate."
                ),
            )

            # STAGE 12: AUDIT_COMMITTED
            from app.routers.decisions import _db_ledger_append

            class _DBBackedLedger:
                def append(self, **kwargs):
                    return _db_ledger_append(db, **kwargs)

            audit_ledger = _DBBackedLedger()
            audit_rec = audit_ledger.append(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                attempt_number=event_row.attempt_number,
                source=event_row.source,
                mode=event_row.mode,
                simulation_id=event_row.simulation_id,
                failure_class=failure_class,
                timing_score=timing.liquidity_timing_score,
                recommended_window=policy_decision.effective_window or ai_context.recommended_window,
                recovery_probability=recovery_pred.probability,
                expected_net_recovery=ev.expected_net_recovery,
                ai_recommendation=ai_context.to_dict(),
                policy_verdict=policy_decision.verdict,
                policy_checks=[c.__dict__ for c in policy_decision.checks],
                action=final_action,
                execution_result=execution_result.to_dict() if execution_result else None,
                outcome=None,
                model_version=recovery_pred.model_version,
                policy_version=policy_decision.policy_version,
                selected_strategy=selected_strategy,
                expected_recovery=expected_recovery,
                expected_utility=expected_utility,
                portfolio_rank=opp.rank if opp else None,
                selected_channel=opp.selected_channel if opp else None,
                nerv=opp.nerv if opp else None,
                intervention_cost=opp.intervention_cost if opp else None,
            )

            lifecycle.emit(
                STAGE_AUDIT_COMMITTED,
                status="OK",
                seq=audit_rec.seq,
                current_hash=audit_rec.current_hash[:16] + "...",
                model_version=audit_rec.model_version,
                policy_version=audit_rec.policy_version,
            )

            # Persist DecisionDB row
            decision_row = DecisionDB(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                customer_id=event_row.customer_id,
                amount=event_row.amount,
                attempt_number=event_row.attempt_number,
                failure_class=failure_class,
                timing_score=timing.liquidity_timing_score,
                recommended_window=timing.to_dict()["recommended_window"],
                recovery_probability=recovery_pred.probability,
                expected_net_recovery=ev.expected_net_recovery,
                ai_recommendation_json=ai_context.to_dict(),
                policy_verdict=policy_decision.verdict,
                policy_checks_json=[c.__dict__ for c in policy_decision.checks],
                action=final_action,
                execution_result_json=execution_result.to_dict() if execution_result else None,
                source=event_row.source,
                mode=event_row.mode,
                simulation_id=event_row.simulation_id,
                model_version=recovery_pred.model_version,
                policy_version=policy_decision.policy_version,
                audit_seq=audit_rec.seq,
                audit_hash=audit_rec.current_hash,
                selected_strategy=selected_strategy,
                expected_recovery=expected_recovery,
                expected_utility=expected_utility,
                selected_channel=opp.selected_channel if opp else None,
                nerv=opp.nerv if opp else None,
                intervention_cost=opp.intervention_cost if opp else None,
                alternatives_json=candidates_summary,
                portfolio_rank=opp.rank if opp else None,
            )
            db.add(decision_row)
            db.commit()

            return AgentExecutionResult(
                event_id=event_row.event_id,
                payment_id=event_row.payment_id,
                status="PROCESSED",
                failure_class=failure_class,
                selected_strategy=selected_strategy,
                policy_verdict=policy_decision.verdict,
                provider_disposition=provider_disposition,
                provider_capability=(execution_result.capability if execution_result else None),
                execution_status=final_action,
                audit_seq=audit_rec.seq,
                audit_hash=audit_rec.current_hash,
                detail=f"Agent completed lifecycle. Action: '{final_action}'.",
                lifecycle_trace=lifecycle.trace,
            )

        except Exception as exc:
            # Roll back uncommitted lifecycle work on every failure path so a
            # caller-provided session is left clean and immediately reusable
            # (own_db sessions are additionally closed in the finally block).
            try:
                db.rollback()
            except Exception:
                pass
            if lifecycle:
                lifecycle.emit_error(STAGE_AUDIT_COMMITTED, error=str(exc))
            else:
                logger.error(json.dumps({
                    "agent": "RecoverSense",
                    "stage": STAGE_EVENT_RECEIVED,
                    "status": "ERROR",
                    "event_id": event_id,
                    "error": str(exc),
                }))
            return AgentExecutionResult(
                event_id=event_id,
                payment_id="UNKNOWN",
                status="ERROR",
                detail=f"Agent runtime encountered error and failed safely: {str(exc)}",
                error=str(exc),
                lifecycle_trace=lifecycle.trace if lifecycle else [],
            )
        finally:
            if own_db:
                db.close()


_global_runtime: Optional[AgentRuntime] = None


def get_agent_runtime() -> AgentRuntime:
    global _global_runtime
    if _global_runtime is None:
        _global_runtime = AgentRuntime()
    return _global_runtime


def process_event_through_pipeline(event_id: str) -> AgentExecutionResult:
    """Convenience entry point to run the agent lifecycle for an event_id."""
    return get_agent_runtime().process_event(event_id)
