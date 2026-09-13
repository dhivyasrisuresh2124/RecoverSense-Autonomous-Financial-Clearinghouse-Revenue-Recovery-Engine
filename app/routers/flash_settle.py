
"""RecoverSense - Flash-Settle API Router
==========================================

Pillar 1: Real-Time Micro-Defeasance Underwriting.

Endpoints to inspect/test Flash-Settle state. Deliberately minimal:
no unrestricted money-transfer endpoint is exposed.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import DecisionDB, FlashSettleExposureDB, PaymentEventDB
from app.routers.decisions import _db_ledger_append
from execution.razorpay_client import normalize_payment_state
from flash_settle import (
    FlashSettleState,
    UnderwritingInputs,
    create_flash_settle_ledger,
    expire_exposure,
    reconcile_exposure,
    underwrite,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flash-settle", tags=["flash-settle"])


def _get_exposure(db: Session, event_id: str) -> Optional[FlashSettleExposureDB]:
    return db.query(FlashSettleExposureDB).filter(
        FlashSettleExposureDB.event_id == event_id
    ).first()


@router.get("/status/{event_id}")
def flash_settle_status(event_id: str, db: Session = Depends(get_db)):
    """Returns the current Flash-Settle exposure state for an event."""
    exposure = _get_exposure(db, event_id)
    if not exposure:
        raise HTTPException(status_code=404, detail=f"No Flash-Settle exposure for event {event_id}")
    return {
        "event_id": exposure.event_id,
        "payment_id": exposure.payment_id,
        "customer_id": exposure.customer_id,
        "merchant_id": exposure.merchant_id,
        "state": exposure.state,
        "advance_amount": exposure.advance_amount,
        "outstanding_amount": exposure.outstanding_amount,
        "recovered_amount": exposure.recovered_amount,
        "recovery_probability": exposure.recovery_probability,
        "recovery_horizon_hours": exposure.recovery_horizon_hours,
        "merchant_reserve_cap": exposure.merchant_reserve_cap,
        "merchant_exposure": exposure.merchant_exposure,
        "expires_at": exposure.expires_at.isoformat() if exposure.expires_at else None,
        "created_at": exposure.created_at.isoformat() if exposure.created_at else None,
        "simulation_id": exposure.simulation_id,
        "source": exposure.source,
    }


@router.get("/exposures")
def list_flash_settle_exposures(
    state: Optional[str] = None,
    merchant_id: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lists Flash-Settle exposures, optionally filtered by state or merchant."""
    query = db.query(FlashSettleExposureDB)
    if state:
        query = query.filter(FlashSettleExposureDB.state == state.upper())
    if merchant_id:
        query = query.filter(FlashSettleExposureDB.merchant_id == merchant_id)
    exposures = query.order_by(FlashSettleExposureDB.created_at.desc()).limit(100).all()
    return {
        "count": len(exposures),
        "exposures": [
            {
                "event_id": e.event_id,
                "payment_id": e.payment_id,
                "state": e.state,
                "advance_amount": e.advance_amount,
                "outstanding_amount": e.outstanding_amount,
                "recovery_probability": e.recovery_probability,
            }
            for e in exposures
        ],
    }


@router.post("/simulate-recovery/{event_id}")
def simulate_recovery(
    event_id: str,
    recovered_amount: Optional[float] = None,
    db: Session = Depends(get_db),
):
    """Explicitly simulates a recovery observation for a Flash-Settle exposure.

    This is a CONTROLLED test/simulation pathway, clearly marked as
    simulated. It transitions AUTHORIZED -> ADVANCE_RECORDED ->
    AWAITING_RECOVERY -> RECOVERY_OBSERVED -> RECONCILED.
    """
    exposure = _get_exposure(db, event_id)
    if not exposure:
        raise HTTPException(status_code=404, detail=f"No Flash-Settle exposure for event {event_id}")

    if exposure.state in {FlashSettleState.RECONCILED.value, FlashSettleState.EXPIRED.value, FlashSettleState.DEFERRED.value}:
        raise HTTPException(
            status_code=409,
            detail=f"Exposure already in terminal state {exposure.state}",
        )

    amount = recovered_amount if recovered_amount is not None else exposure.advance_amount
    internal = create_flash_settle_ledger(
        event_id=exposure.event_id,
        payment_id=exposure.payment_id,
        customer_id=exposure.customer_id,
        advance_amount=exposure.advance_amount,
        merchant_reserve_cap=exposure.merchant_reserve_cap,
        recovery_horizon_hours=exposure.recovery_horizon_hours,
    )
    result = reconcile_exposure(internal, amount)

    if result["reconciled"]:
        exposure.state = FlashSettleState.RECONCILED.value
        exposure.recovered_amount = result["recovered_amount"]
        exposure.outstanding_amount = result["remaining_outstanding"]
        exposure.reconciliation_outcome_json = result
        exposure.source = "SIMULATOR"
        logger.info(
            "flash_settle_reconciled",
            extra={
                "event_id": event_id,
                "recovered": result["recovered_amount"],
                "applied": result["applied_to_outstanding"],
            },
        )
    db.commit()
    return {
        "event_id": event_id,
        "state": exposure.state,
        "recovery_simulated": True,
        "outcome": result,
    }


@router.post("/simulate-expiry/{event_id}")
def simulate_expiry(event_id: str, db: Session = Depends(get_db)):
    """Explicitly simulates the expiry of a Flash-Settle exposure after the
    72-hour window with no recovery observed.

    Transitions to EXPIRED. No further autonomous advance is possible.
    """
    exposure = _get_exposure(db, event_id)
    if not exposure:
        raise HTTPException(status_code=404, detail=f"No Flash-Settle exposure for event {event_id}")

    if exposure.state in {FlashSettleState.RECONCILED.value, FlashSettleState.EXPIRED.value, FlashSettleState.DEFERRED.value}:
        raise HTTPException(
            status_code=409,
            detail=f"Exposure already in terminal state {exposure.state}",
        )

    internal = create_flash_settle_ledger(
        event_id=exposure.event_id,
        payment_id=exposure.payment_id,
        customer_id=exposure.customer_id,
        advance_amount=exposure.advance_amount,
        merchant_reserve_cap=exposure.merchant_reserve_cap,
        recovery_horizon_hours=exposure.recovery_horizon_hours,
    )
    result = expire_exposure(internal)
    if result["expired"]:
        exposure.state = FlashSettleState.EXPIRED.value
        exposure.reconciliation_outcome_json = result
        exposure.source = "SIMULATOR"
        logger.info("flash_settle_expired", extra={"event_id": event_id, "outstanding": result["outstanding_amount"]})
    db.commit()
    return {
        "event_id": event_id,
        "state": exposure.state,
        "expiry_simulated": True,
        "outcome": result,
    }


@router.get("/underwriting/{event_id}")
def get_underwriting(event_id: str, db: Session = Depends(get_db)):
    """Returns the snapshotted underwriting result for an authorized exposure."""
    exposure = _get_exposure(db, event_id)
    if not exposure:
        raise HTTPException(status_code=404, detail=f"No Flash-Settle exposure for event {event_id}")
    if not exposure.underwriting_result_json:
        raise HTTPException(status_code=404, detail="No underwriting snapshot available")
    return {
        "event_id": event_id,
        "underwriting": exposure.underwriting_result_json,
    }
