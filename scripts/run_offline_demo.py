#!/usr/bin/env python3
"""
RecoverSense — Offline End-to-End Demo

Runs the COMPLETE decision pipeline (generation -> classification ->
timing -> AI context -> recovery probability -> expected value -> policy
-> execution -> audit) for a handful of synthetic events, with zero
external dependencies (no FastAPI, no DB, no network, no LLM API key
required). Useful for a quick sanity check or a terminal demo.

Usage:
    python scripts/run_offline_demo.py [n_events]
"""

import json
import os
import sys

BACKEND = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend")
sys.path.insert(0, BACKEND)

from data.generator import SyntheticDataEngine
from models.recovery_model import RecoveryModel
from execution.razorpay_client import SimulatorExecutionAdapter
from audit.audit_ledger import AuditLedger
from decision_pipeline import run_decision


def main():
    n_events = int(sys.argv[1]) if len(sys.argv) > 1 else 8

    print(f"RecoverSense — Offline Demo ({n_events} synthetic events)\n" + "=" * 60)

    engine = SyntheticDataEngine(seed=123)
    events = engine.generate(n_events)

    recovery_model = RecoveryModel()  # untrained fallback scorer for a quick demo;
                                       # run scripts/run_evaluation.py to train + evaluate properly
    execution_adapter = SimulatorExecutionAdapter()
    ledger = AuditLedger()

    for i, event in enumerate(events, 1):
        print(f"\n--- Event {i}/{n_events}: {event['event_id']} "
              f"(customer {event['customer_id']}, ₹{event['amount']:,.2f}, "
              f"{event['failure_reason']}) ---")

        output = run_decision(
            event,
            recovery_model=recovery_model,
            execution_adapter=execution_adapter,
            audit_ledger=ledger,
        )

        print(f"  Failure class:         {output.failure_class}")
        print(f"  Liquidity timing score: {output.timing.liquidity_timing_score:.3f} "
              f"({output.timing.confidence_label}) -> window "
              f"{output.timing.to_dict()['recommended_window']}")
        print(f"  AI recommendation:      {output.ai_context.recommended_action} "
              f"(intent={output.ai_context.customer_intent}, source={output.ai_context.source})")
        print(f"  Recovery probability:   {output.recovery.probability:.3f} ({output.recovery.confidence})")
        print(f"  Expected net recovery:  ₹{output.expected_value.expected_net_recovery:,.2f}")
        print(f"  Policy verdict:         {output.policy.verdict} — {output.policy.reason_summary}")
        if output.execution:
            print(f"  Execution:              {output.execution.status} ({output.execution.detail})")

    print("\n" + "=" * 60)
    chain_report = ledger.verify_chain()
    print(f"Audit chain: {chain_report['n_records']} records, valid={chain_report['valid']}")
    print("\nFull audit trail:")
    print(json.dumps(ledger.to_list(), indent=2)[:3000])


if __name__ == "__main__":
    main()
