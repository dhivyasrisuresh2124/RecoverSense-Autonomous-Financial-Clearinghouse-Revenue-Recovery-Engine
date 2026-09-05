#!/usr/bin/env python3
"""
RecoverSense — Evaluation Runner (CLI)

Trains the recovery model and runs the full baseline-vs-RecoverSense
benchmark on a held-out synthetic test set. Prints results and writes
them to sample_output/evaluation_results.json.

Usage:
    python scripts/run_evaluation.py [n_dev] [n_test] [seed]
"""

import json
import os
import sys


def configure_utf8_output():
    """Keep currency-bearing CLI output safe in Windows legacy consoles."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

BACKEND = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend")
sys.path.insert(0, BACKEND)

from evaluation.evaluate import run_full_evaluation, run_multi_seed_evaluation


def main():
    configure_utf8_output()
    if len(sys.argv) > 1 and sys.argv[1] == "--multi-seed":
        n_dev = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
        n_test = int(sys.argv[3]) if len(sys.argv) > 3 else 1200
        result = run_multi_seed_evaluation(n_dev=n_dev, n_test=n_test)
        out_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "sample_output", "multi_seed_evaluation_results.json",
        )
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print("\n" + "=" * 60)
        print(f"MULTI-SEED EVALUATION — {len(result['seeds'])} independent seeds")
        print("=" * 60)
        vf, vp = result["incremental_net_recovery_vs_fixed"], result["incremental_net_recovery_vs_peak_hour"]
        print(f"vs. Fixed-Timer:    mean ₹{vf['mean']:,.2f}  "
              f"95% CI [₹{vf['ci_95_low']:,.2f}, ₹{vf['ci_95_high']:,.2f}]")
        print(f"vs. Peak-Hour:      mean ₹{vp['mean']:,.2f}  "
              f"95% CI [₹{vp['ci_95_low']:,.2f}, ₹{vp['ci_95_high']:,.2f}]")
        print(f"\nFull results written to {out_path}")
        return

    n_dev = int(sys.argv[1]) if len(sys.argv) > 1 else 6000
    n_test = int(sys.argv[2]) if len(sys.argv) > 2 else 1200
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42

    result = run_full_evaluation(n_dev=n_dev, n_test=n_test, seed=seed)

    out_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "sample_output", "evaluation_results.json",
    )
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print("\n" + "=" * 60)
    print("SYNTHETIC CONTROLLED EVALUATION — held-out test set")
    print("=" * 60)
    print(f"Fixed-Timer Baseline    recovery rate: {result['baseline_fixed']['recovery_rate_pct']}%  "
          f"net recovered: ₹{result['baseline_fixed']['net_recovered_revenue']:,.2f}")
    print(f"Population Peak-Hour ({result['population_peak_hour']}:00) baseline  recovery rate: "
          f"{result['baseline_peak_hour']['recovery_rate_pct']}%  "
          f"net recovered: ₹{result['baseline_peak_hour']['net_recovered_revenue']:,.2f}")
    print(f"RecoverSense (personalized) recovery rate: {result['recoversense']['recovery_rate_pct']}%  "
          f"net recovered: ₹{result['recoversense']['net_recovered_revenue']:,.2f}")
    print(f"\nIncremental Net Revenue Recovered vs. Fixed-Timer:   ₹{result['incremental_net_recovery_vs_fixed']:,.2f}")
    print(f"Incremental Net Revenue Recovered vs. Peak-Hour:     ₹{result['incremental_net_recovery_vs_peak_hour']:,.2f}")
    print(f"\nFull results written to {out_path}")


if __name__ == "__main__":
    main()
