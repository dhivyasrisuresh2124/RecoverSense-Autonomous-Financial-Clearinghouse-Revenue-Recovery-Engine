import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter
from execution.razorpay_client import SimulatorExecutionAdapter
from agent.context_reasoner import validate_schema, SchemaValidationError
from policy.policy_engine import evaluate_policy, PolicyConfig

router = APIRouter(prefix="/failure-demo", tags=["failure-demo"])


@router.post("/api-timeout")
def demo_api_timeout():
    adapter = SimulatorExecutionAdapter(inject_timeout_rate=1.0)
    result = adapter.schedule_retry("pay_timeout_demo", "2026-08-05T18:00:00", idempotency_key="demo_timeout")
    return {"scenario": "api_timeout", "result": result.to_dict()}


@router.post("/duplicate-webhook")
def demo_duplicate_webhook():
    adapter = SimulatorExecutionAdapter()
    key = "pay_dup_demo:1"
    r1 = adapter.schedule_retry("pay_dup_demo", "2026-08-05T18:00:00", idempotency_key=key)
    r2 = adapter.schedule_retry("pay_dup_demo", "2026-08-05T18:00:00", idempotency_key=key)
    return {"scenario": "duplicate_webhook", "first_call": r1.to_dict(), "second_call": r2.to_dict()}


@router.post("/malformed-llm-output")
def demo_malformed_llm_output():
    bad = {"customer_intent": "SUPER_HIGH", "failure_context": "TEMPORARY",
           "recommended_action": "SCHEDULE_RETRY", "recommended_window": None, "reason": "x"}
    try:
        validate_schema(bad)
        return {"scenario": "malformed_llm_output", "accepted": True}
    except SchemaValidationError as e:
        return {"scenario": "malformed_llm_output", "accepted": False, "rejection_reason": str(e)}


@router.post("/policy-rejection")
def demo_policy_rejection():
    decision = evaluate_policy(
        recommended_action="SCHEDULE_RETRY", attempt_number=4, failure_class="SOFT",
        amount=5000, expected_net_recovery=1000, recommended_window="18:00-20:00",
        already_actioned_recently=False, config=PolicyConfig(max_retry_attempts=3),
    )
    return {"scenario": "policy_rejection", "decision": decision.to_dict()}
