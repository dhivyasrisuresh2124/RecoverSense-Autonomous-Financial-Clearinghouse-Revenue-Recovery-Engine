import os

RUNTIME_CODE = '''"""
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
from app.models_db import DecisionDB, PaymentEventDB
from audit.audit_ledger import AuditLedger, AuditRecord
from execution.razorpay_client import (
    ExecutionAdapter,
    ExecutionResult,
    ProviderCapability,
    get_execution_adapter_for_event,
    normalize_payment_state,
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
                    detail=f"Payment event \'{event_id}\' not found in database.",
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
                    detail=f"Event {event_id} was already processed with action \'{existing_decision.action}\'.",
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
            )
            opp = opportunities[0] if opportunities else None
            candidates_summary = [
                {"strategy": c["strategy"], "eligible": c["eligible"], "expected_utility": c.get("expected_utility", 0.0)}
                for c in (opp.alternatives if opp else [])
            ]
            lifecycle.emit(
                STAGE_STRATEGIES_EVALUATED,
                status="OK",
                n_candidates=len(candidates_summary),
                candidates=candidates_summary,
            )

            # STAGE 6: ACTION_SELECTED
            selected_strategy = opp.selected_strategy if opp else "STOP"
            expected_utility = opp.expected_utility if opp else 0.0
            expected_recovery = opp.expected_recovery if opp else 0.0
            policy_recommendation = opp.policy_recommendation if opp else "NO_ACTION"
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

            if policy_decision.verdict == "APPROVE" and ai_context.recommended_action == "SCHEDULE_RETRY":
                current_state = adapter.get_payment_state(execution_payment_id)
                state_safety = normalize_payment_state(current_state)
                provider_status = state_safety.normalized_status
                provider_disposition = state_safety.disposition

                if state_safety.disposition == "NEVER_RETRY":
                    execution_result = ExecutionResult(
                        status="SKIPPED_STALE_STATE",
                        adapter=getattr(adapter, "name", "unknown"),
                        idempotency_key=idempotency_key,
                        detail=f"Skipped execution — {state_safety.detail} (normalized: \'{state_safety.normalized_status}\').",
                        capability=getattr(adapter, "check_capability", lambda op: ProviderCapability.SIMULATED_OPERATION)("schedule_retry").value,
                    )
                    final_action = execution_result.status
                elif state_safety.disposition == "DEFER":
                    execution_result = ExecutionResult(
                        status="DEFERRED_PROVIDER_STATE",
                        adapter=getattr(adapter, "name", "unknown"),
                        idempotency_key=idempotency_key,
                        detail=f"Deferred execution safely — {state_safety.detail} (normalized: \'{state_safety.normalized_status}\').",
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
                        execution_result = adapter.schedule_retry(
                            payment_id=execution_payment_id,
                            window_start_iso=f"{str(event_row.timestamp)[:10]}T{window_start}:00",
                            idempotency_key=idempotency_key,
                        )
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
            lifecycle.emit(
                STAGE_OUTCOME_OBSERVED,
                status="OK",
                final_action=final_action,
                execution_status=(execution_result.status if execution_result else "NOT_EXECUTED"),
                recovery_probability=round(recovery_pred.probability, 4),
                expected_net_recovery=round(ev.expected_net_recovery, 2),
            )

            # STAGE 11: LEARNING_UPDATED
            lifecycle.emit(
                STAGE_LEARNING_UPDATED,
                status="OK",
                strategy=selected_strategy,
                expected_recovery=round(expected_recovery, 2),
                learning_active=True,
                note="Strategy performance tracker is active. Observable payment outcomes update Bayesian calibration.",
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
            if own_db:
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
'''

target_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend", "agent", "runtime.py")
with open(target_file, "w", encoding="utf-8") as f:
    f.write(RUNTIME_CODE)

init_code = '''from agent.runtime import (
    AgentRuntime,
    AgentExecutionResult,
    AgentLifecycleLogger,
    get_agent_runtime,
    process_event_through_pipeline,
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
    ALL_LIFECYCLE_STAGES,
)
'''
init_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend", "agent", "__init__.py")
with open(init_file, "w", encoding="utf-8") as f:
    f.write(init_code)

loop_code = '''# Backward compatibility alias for agent loop
from agent.runtime import (
    process_event_through_pipeline,
    AgentRuntime,
    AgentLifecycleLogger,
    get_agent_runtime,
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
    ALL_LIFECYCLE_STAGES,
)
'''
loop_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend", "agent", "agent_loop.py")
with open(loop_file, "w", encoding="utf-8") as f:
    f.write(loop_code)

print(f"Created runtime, __init__, and agent_loop successfully")
