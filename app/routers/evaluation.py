import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import EvaluationRunRequest
from app.models_db import EvaluationResultDB

router = APIRouter(prefix="/evaluation", tags=["evaluation"])

@router.post("/run")
def run_evaluation(req: EvaluationRunRequest, db: Session = Depends(get_db)):
    """
    Runs the full reproducible benchmark: Fixed-Timer Baseline vs
    RecoverSense, on a freshly generated held-out synthetic test set.
    SYNTHETIC CONTROLLED EVALUATION — see README for disclosure.
    """
    from evaluation.evaluate import run_full_evaluation
    result = run_full_evaluation(n_dev=req.n_dev, n_test=req.n_test, seed=req.seed)
    stored = db.get(EvaluationResultDB, 1)
    if stored is None:
        db.add(EvaluationResultDB(id=1, result_json=result))
    else:
        stored.result_json = result
    db.commit()
    return result


@router.get("/latest")
def get_latest_evaluation(db: Session = Depends(get_db)):
    stored = db.get(EvaluationResultDB, 1)
    if stored is None:
        return {"status": "no_evaluation_run_yet"}
    return stored.result_json


@router.get("/decision-trace/{event_id}")
def get_decision_trace(event_id: str):
    """
    Returns a structured decision trace for a recovery opportunity.

    Trace stages:
      MODEL_PROPOSAL → POLICY_AUTHORIZATION → PROVIDER_VERIFICATION →
      EXECUTION_STATUS → OBSERVED_OUTCOME → INTERNAL_LEDGER_ONLY →
      UNSUPPORTED_PROVIDER_OPERATION
    """
    from evaluation.observability import build_full_trace
    result = build_full_trace(event_id)
    if not result or not result.get("main_decision"):
        raise HTTPException(status_code=404, detail="Event not found")
    return result


@router.get("/summary")
def get_evaluation_summary():
    """
    Compact evaluation summary derived from persisted records only.
    """
    from evaluation.observability import build_evaluation_summary
    return build_evaluation_summary()


@router.get("/strategy-comparison")
def get_strategy_comparison():
    """
    Strategy-level comparison using existing observation records.
    Only strategies with available data are shown.
    """
    from evaluation.observability import build_strategy_comparison
    return build_strategy_comparison()


@router.get("/audit/verify")
def verify_audit_integrity():
    """
    Re-walks the stored audit chain without mutating any record.
    """
    from evaluation.observability import verify_audit_chain
    return verify_audit_chain()


@router.get("/batch-metrics")
def get_batch_metrics():
    """
    Batch evaluation metrics: Revenue at Risk, Expected Gross Recovery,
    Expected NERV, Intervention Spend, and NERV margin.
    """
    from evaluation.observability import build_batch_metrics
    return build_batch_metrics()
