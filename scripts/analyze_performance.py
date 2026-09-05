#!/usr/bin/env python3
"""Offline diagnostic companion for the existing deterministic evaluator.

It intentionally does not import application routers or mutate application state.
Ground truth is used only after decisions for counterfactual evaluation analysis.
"""
import json
import os
import sys
from collections import Counter, defaultdict

BACKEND = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend")
sys.path.insert(0, BACKEND)

from data.generator import generate_dataset, SyntheticDataEngine
from evaluation.evaluate import train_model, evaluate_strategy
from evaluation.baseline import compute_population_peak_hour
from models.failure_classifier import classify_failure
from models.timing_model import compute_timing_score
from models.expected_value import compute_expected_net_recovery
from agent.context_reasoner import get_context_recommendation
from policy.policy_engine import evaluate_policy
from optimization.recovery_optimizer import optimize_portfolio


STATUSES = ("ACTION_SELECTED", "DEFERRED", "UNALLOCATED", "ESCALATED", "STOPPED", "POLICY_BLOCKED")


def analyse(seed=42, n_dev=6000, n_test=1200, budget=10):
    data = generate_dataset(n_dev=n_dev, n_test=n_test, seed=seed)
    dev_engine = SyntheticDataEngine(seed=data["dev_engine_seed"])
    test_engine = SyntheticDataEngine(seed=data["test_engine_seed"])
    model = train_model(data["dev_events"], dev_engine)
    analyses, details = [], {}
    for event in data["test_events"]:
        failure_class = event.get("failure_class") or classify_failure(event["failure_reason"])
        timing = compute_timing_score(event.get("metadata", {}).get("history", []))
        context = get_context_recommendation(event.get("metadata", {}).get("support_note", ""), failure_class,
                                             timing.to_dict()["recommended_window"])
        pred = model.predict(timing_score=timing.liquidity_timing_score,
            scheduled_hour=timing.circular_mean_hour, circular_mean_hour=timing.circular_mean_hour,
            n_history_points=timing.n_history_points, failure_class=failure_class,
            attempt_number=event.get("attempt_number", 1), high_intent_signal=context.customer_intent == "HIGH",
            failure_reason=event["failure_reason"])
        analyses.append({"event": event, "failure_class": failure_class,
            "timing_score": timing.liquidity_timing_score, "timing_window": timing.to_dict()["recommended_window"],
            "recovery_probability": pred.probability})
        details[event["event_id"]] = (timing, context, pred)

    opportunities = optimize_portfolio(analyses, max_actions_per_cycle=budget)
    records = []
    for op in opportunities:
        timing, context, pred = details[op.event["event_id"]]
        status = None
        policy = None
        if op.allocated:
            policy = evaluate_policy(recommended_action=context.recommended_action,
                attempt_number=op.event.get("attempt_number", 1), failure_class=op.failure_class,
                amount=op.event["amount"], expected_net_recovery=compute_expected_net_recovery(
                    op.event["amount"], pred.probability).expected_net_recovery,
                recommended_window=context.recommended_window, already_actioned_recently=False)
            status = "ACTION_SELECTED" if policy.verdict == "APPROVE" else "POLICY_BLOCKED"
        elif op.selected_strategy == "ESCALATE": status = "ESCALATED"
        elif op.selected_strategy == "STOP": status = "STOPPED"
        elif any("action budget was allocated" in x for x in op.explanation): status = "UNALLOCATED"
        else: status = "DEFERRED"
        hour = timing.circular_mean_hour if timing.circular_mean_hour is not None else 14.0
        recoverable = test_engine.label_ground_truth_recovery(op.event, hour)
        records.append({"event": op.event, "status": status, "strategy": op.selected_strategy,
            "rank": op.rank, "probability": pred.probability, "expected_recovery": op.expected_recovery,
            "expected_utility": op.expected_utility, "failure_class": op.failure_class,
            "timing_score": timing.liquidity_timing_score, "has_timing_window": bool(timing.to_dict()["recommended_window"]),
            "history_points": timing.n_history_points, "recoverable": recoverable,
            "actual_recoverable_amount": op.event["amount"] if recoverable else 0.0,
            "policy_verdict": policy.verdict if policy else None})
    return records, data, test_engine, model


def average(rows, key): return round(sum(row[key] for row in rows) / len(rows), 4) if rows else 0.0


def group_summary(rows, field):
    summary = {}
    for value in sorted({str(row[field]) for row in rows}):
        group = [r for r in rows if str(r[field]) == value]
        summary[value] = {
            "count": len(group), "revenue_at_risk": round(sum(r["event"]["amount"] for r in group), 2),
            "mean_payment_amount": round(average(group, "event_amount") if False else sum(r["event"]["amount"] for r in group) / len(group), 2),
            "actual_recoverable_amount": round(sum(r["actual_recoverable_amount"] for r in group), 2),
            "recovery_rate_pct": round(100 * sum(r["recoverable"] for r in group) / len(group), 2),
            "mean_predicted_probability": average(group, "probability"),
            "expected_recovery": round(sum(r["expected_recovery"] for r in group), 2),
            "expected_utility": round(sum(r["expected_utility"] for r in group), 2),
            "failure_classes": dict(Counter(r["failure_class"] for r in group)),
            "mean_timing_score": average(group, "timing_score"),
            "timing_window_rate_pct": round(100 * sum(r["has_timing_window"] for r in group) / len(group), 2),
            "mean_history_points": average(group, "history_points"),
            "strategies": dict(Counter(r["strategy"] for r in group)),
        }
    return summary


def frontier(seed, n_dev, n_test, budgets):
    results = {}
    baseline_data = generate_dataset(n_dev=n_dev, n_test=n_test, seed=seed)
    base_engine = SyntheticDataEngine(seed=baseline_data["test_engine_seed"])
    fixed = evaluate_strategy(baseline_data["test_events"], base_engine, "baseline_fixed")
    peak = evaluate_strategy(baseline_data["test_events"], base_engine, "baseline_peak_hour",
        population_peak_hour=compute_population_peak_hour(baseline_data["dev_events"]))
    for budget in budgets:
        rows, _, _, _ = analyse(seed, n_dev, n_test, budget)
        selected = [r for r in rows if r["status"] == "ACTION_SELECTED"]
        actual = sum(r["actual_recoverable_amount"] for r in selected)
        results[str(budget)] = {
            "selected": len(selected), "actual_recovered": round(actual, 2),
            "recovery_rate_pct": round(100 * sum(r["recoverable"] for r in selected) / len(selected), 2) if selected else 0,
            "recovery_per_action": round(actual / len(selected), 2) if selected else 0,
            "expected_recovery": round(sum(r["expected_recovery"] for r in selected), 2),
            "incremental_actual_vs_fixed": round(actual - fixed["actual_recovered_revenue"], 2),
            "incremental_actual_vs_peak": round(actual - peak["actual_recovered_revenue"], 2),
            "policy_blocked": sum(r["status"] == "POLICY_BLOCKED" for r in rows),
            "deferred": sum(r["status"] == "DEFERRED" for r in rows),
            "unallocated": sum(r["status"] == "UNALLOCATED" for r in rows),
        }
    return results


def main():
    rows, _, _, _ = analyse()
    selected = [r for r in rows if r["status"] == "ACTION_SELECTED"]
    calibration = []
    for low in range(10):
        bucket = [r for r in rows if low / 10 <= r["probability"] < (low + 1) / 10]
        if bucket:
            predicted, actual = average(bucket, "probability"), sum(r["recoverable"] for r in bucket) / len(bucket)
            calibration.append({"bucket": f"{low/10:.1f}-{(low+1)/10:.1f}", "count": len(bucket),
                "mean_predicted": predicted, "actual_rate": round(actual, 4),
                "calibration_error_actual_minus_predicted": round(actual - predicted, 4)})
    ranked = sorted(rows, key=lambda r: (r["rank"], r["event"]["payment_id"], r["event"]["event_id"]))
    total_recoverable = sum(r["actual_recoverable_amount"] for r in rows)
    ranking = {}
    for k in (10, 50, 100, 120, 300, 600, 1200):
        subset = ranked[:k]
        captured = sum(r["actual_recoverable_amount"] for r in subset)
        ranking[str(k)] = {"actual_recoverable_captured": round(captured, 2),
            "share_of_total_recoverable_pct": round(100 * captured / total_recoverable, 2) if total_recoverable else 0,
            "event_recovery_rate_pct": round(100 * sum(r["recoverable"] for r in subset) / len(subset), 2)}
    report = {"seed": 42, "n_dev": 6000, "n_test": 1200, "budget": 10,
        "decision_distribution": group_summary(rows, "status"),
        "strategy_distribution": group_summary(rows, "strategy"),
        "selected_expected_vs_actual": group_summary(selected, "strategy"),
        "calibration": calibration, "ranking": ranking,
        "total_counterfactual_recoverable": round(total_recoverable, 2),
        "budget_frontier": frontier(42, 6000, 1200, [5, 10, 25, 50, 100, 250, 500, 909]),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
