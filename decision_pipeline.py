"""
RecoverSense — Decision Pipeline

Orchestrates the full flow for a single payment failure event:

  Event -> Failure Classification -> Liquidity Timing Model
        -> AI Context Reasoner -> Recovery Probability Model
        -> Expected Net Recovery -> Policy Gate -> Execution -> Audit

This module has no FastAPI/DB dependency so it can run in:
  - the offline demo / evaluation scripts (this sandbox, no network)
  - the FastAPI app (backend/app/routers/*), which wraps it with
    persistence and HTTP concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional

from models.failure_classifier import classify_failure
from models.timing_model import compute_timing_score, TimingResult
from models.recovery_model import RecoveryModel, RecoveryPrediction
from models.expected_value import compute_expected_net_recovery, CostAssumptions, ExpectedValueResult
from agent.context_reasoner import get_context_recommendation, ContextReasonerOutput
from policy.policy_engine import evaluate_policy, PolicyConfig, PolicyDecision
from execution.razorpay_client import ExecutionAdapter, ExecutionResult, normalize_payment_state, ProviderCapability
from audit.audit_ledger import AuditLedger, AuditRecord


@dataclass
class DecisionOutput:
    event_id: str
    payment_id: str
    failure_class: str
    timing: TimingResult
    ai_context: ContextReasonerOutput
    recovery: RecoveryPrediction
    expected_value: ExpectedValueResult
    policy: PolicyDecision
    execution: Optional[ExecutionResult]
    audit_record: AuditRecord

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "payment_id": self.payment_id,
            "failure_class": self.failure_class,
            "timing": self.timing.to_dict(),
            "ai_context": self.ai_context.to_dict(),
            "recovery": self.recovery.to_dict(),
            "expected_value": self.expected_value.to_dict(),
            "policy": self.policy.to_dict(),
            "execution": self.execution.to_dict() if self.execution else None,
            "audit": {"seq": self.audit_record.seq, "current_hash": self.audit_record.current_hash},
        }


def run_decision(
    event: Dict[str, Any],
    *,
    recovery_model: RecoveryModel,
    execution_adapter: ExecutionAdapter,
    audit_ledger: AuditLedger,
    policy_config: Optional[PolicyConfig] = None,
    cost_assumptions: Optional[CostAssumptions] = None,
    already_actioned_recently: bool = False,
) -> DecisionOutput:
    policy_config = policy_config or PolicyConfig()
    cost_assumptions = cost_assumptions or CostAssumptions()

    # DEFENSE IN DEPTH: strip any ground-truth-only fields the caller may
    # have forgotten to remove (see data/generator.py::strip_ground_truth
    # and GROUND_TRUTH_ONLY_KEYS). The evaluation harness is the only
    # legitimate holder of these fields, and it never passes them here —
    # this line makes that a structural guarantee, not a convention.
    from data.generator import strip_ground_truth
    event = strip_ground_truth(event)

    failure_class = event.get("failure_class") or classify_failure(event["failure_reason"])
    source = event.get("source", "PRODUCTION")
    mode = event.get("mode")
    simulation_id = event.get("simulation_id")

    history = event.get("metadata", {}).get("history", [])
    timing = compute_timing_score(history)

    support_note = event.get("metadata", {}).get("support_note", "")
    ai_context = get_context_recommendation(
        support_note=support_note,
        failure_class=failure_class,
        timing_window=timing.to_dict()["recommended_window"],
    )

    scheduled_hour = timing.circular_mean_hour
    recovery = recovery_model.predict(
        timing_score=timing.liquidity_timing_score,
        scheduled_hour=scheduled_hour,
        circular_mean_hour=timing.circular_mean_hour,
        n_history_points=timing.n_history_points,
        failure_class=failure_class,
        attempt_number=event.get("attempt_number", 1),
        high_intent_signal=(ai_context.customer_intent == "HIGH"),
        failure_reason=event["failure_reason"],
    )

    ev = compute_expected_net_recovery(
        amount=event["amount"],
        recovery_probability=recovery.probability,
        costs=cost_assumptions,
    )

    policy_decision = evaluate_policy(
        recommended_action=ai_context.recommended_action,
        attempt_number=event.get("attempt_number", 1),
        failure_class=failure_class,
        amount=event["amount"],
        expected_net_recovery=ev.expected_net_recovery,
        recommended_window=ai_context.recommended_window,
        already_actioned_recently=already_actioned_recently,
        config=policy_config,
    )

    execution_result: Optional[ExecutionResult] = None
    final_action = policy_decision.verdict

    if policy_decision.verdict == "APPROVE" and ai_context.recommended_action == "SCHEDULE_RETRY":
        is_simulation = source.upper() == "SIMULATOR" or (mode or "").upper() == "SIMULATION"
        execution_payment_id = event["event_id"] if is_simulation else event["payment_id"]
        idempotency_key = (
            f"{event['event_id']}:{event.get('attempt_number', 1)}"
            if is_simulation else f"{event['payment_id']}:{event.get('attempt_number', 1)}"
        )
        # STALE-STATE PROTECTION: re-check the payment's current status
        # immediately before executing. If the customer already paid
        # through another channel between the decision and this instant
        # (a real race in production — webhooks and manual payments can
        # both land), we must NOT fire a redundant retry.
        current_state = execution_adapter.get_payment_state(execution_payment_id)
        state_safety = normalize_payment_state(current_state)
        if state_safety.disposition == "NEVER_RETRY":
            execution_result = ExecutionResult(
                status="SKIPPED_STALE_STATE",
                adapter=getattr(execution_adapter, "name", "unknown"),
                idempotency_key=idempotency_key,
                detail=(
                    f"Skipped execution — current payment state is already "
                    f"{state_safety.detail} (normalized state: '{state_safety.normalized_status}')."
                ),
            )
            final_action = execution_result.status
        elif state_safety.disposition == "DEFER":
            execution_result = ExecutionResult(
                status="DEFERRED_PROVIDER_STATE",
                adapter=getattr(execution_adapter, "name", "unknown"),
                idempotency_key=idempotency_key,
                detail=(f"Deferred execution safely: {state_safety.detail} "
                        f"(normalized state: '{state_safety.normalized_status}')."),
                raw_response=current_state,
            )
            final_action = execution_result.status
        else:
            capability = getattr(execution_adapter, "check_capability", lambda op: ProviderCapability.SIMULATED_OPERATION)("schedule_retry")
            if capability == ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION:
                execution_result = ExecutionResult(
                    status="UNSUPPORTED_PROVIDER_OPERATION",
                    adapter=getattr(execution_adapter, "name", "unknown"),
                    idempotency_key=idempotency_key,
                    detail="Provider operation schedule_retry is not supported by the upstream provider. Bounded safely without fake execution.",
                    capability=ProviderCapability.UNSUPPORTED_PROVIDER_OPERATION.value,
                )
                final_action = execution_result.status
            elif capability == ProviderCapability.SAFE_DEFER:
                execution_result = ExecutionResult(
                    status="DEFERRED_PROVIDER_STATE",
                    adapter=getattr(execution_adapter, "name", "unknown"),
                    idempotency_key=idempotency_key,
                    detail="Provider capability check requires deferral.",
                    capability=ProviderCapability.SAFE_DEFER.value,
                )
                final_action = execution_result.status
            else:
                effective_exec_window = policy_decision.effective_window or ai_context.recommended_window
                window_start = effective_exec_window.split("-")[0] if effective_exec_window else "18:00"
                execution_result = execution_adapter.schedule_retry(
                    payment_id=execution_payment_id,
                    window_start_iso=f"{event['timestamp'][:10]}T{window_start}:00",
                    idempotency_key=idempotency_key,
                )
                final_action = execution_result.status

    audit_record = audit_ledger.append(
        event_id=event["event_id"],
        payment_id=event["payment_id"],
        attempt_number=event.get("attempt_number", 1),
        source=source,
        mode=mode,
        simulation_id=simulation_id,
        failure_class=failure_class,
        timing_score=timing.liquidity_timing_score,
        recommended_window=policy_decision.effective_window or ai_context.recommended_window,
        recovery_probability=recovery.probability,
        expected_net_recovery=ev.expected_net_recovery,
        ai_recommendation=ai_context.to_dict(),
        policy_verdict=policy_decision.verdict,
        policy_checks=[c.__dict__ for c in policy_decision.checks],
        action=final_action,
        execution_result=execution_result.to_dict() if execution_result else None,
        outcome=None,
        model_version=recovery.model_version,
        policy_version=policy_decision.policy_version,
    )

    return DecisionOutput(
        event_id=event["event_id"],
        payment_id=event["payment_id"],
        failure_class=failure_class,
        timing=timing,
        ai_context=ai_context,
        recovery=recovery,
        expected_value=ev,
        policy=policy_decision,
        execution=execution_result,
        audit_record=audit_record,
    )
