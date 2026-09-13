from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.models_db import DecisionDB, PaymentEventDB

EXECUTABLE_ACTIONS = {"SCHEDULED", "EXECUTED"}
SIMULATION_SOURCES = {"SIMULATOR", "SIMULATION"}


@dataclass
class DuplicateCheckResult:
    is_duplicate: bool
    matched_event_id: Optional[str] = None
    matched_action: Optional[str] = None
    detail: str = ""


def is_simulation_source(source: Optional[str], mode: Optional[str] = None) -> bool:
    source_value = (source or "").upper()
    mode_value = (mode or "").upper()
    return source_value in SIMULATION_SOURCES or mode_value == "SIMULATION"


def _safe_parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def is_duplicate_action(
    db: Session,
    *,
    payment_id: str,
    attempt_number: int,
    recommended_action: str,
    source: Optional[str],
    mode: Optional[str],
    dedupe_window_minutes: int,
    now: Optional[datetime] = None,
) -> DuplicateCheckResult:
    if recommended_action != "SCHEDULE_RETRY":
        return DuplicateCheckResult(is_duplicate=False, detail="No executable retry action requested.")

    if is_simulation_source(source, mode):
        return DuplicateCheckResult(
            is_duplicate=False,
            detail="Simulation mode uses isolated synthetic runs and does not inherit prior duplicate state.",
        )

    current_time = now or datetime.utcnow()
    window_start = current_time - timedelta(minutes=dedupe_window_minutes)

    candidates = (
        db.query(DecisionDB, PaymentEventDB)
        .join(PaymentEventDB, PaymentEventDB.event_id == DecisionDB.event_id)
        .filter(DecisionDB.payment_id == payment_id)
        .filter(DecisionDB.attempt_number == attempt_number)
        .filter(DecisionDB.action.in_(tuple(EXECUTABLE_ACTIONS)))
        .order_by(DecisionDB.created_at.desc())
        .all()
    )

    for decision, event in candidates:
        event_time = _safe_parse_iso8601(event.timestamp)
        created_time = decision.created_at
        effective_time = event_time or created_time
        if effective_time is None or effective_time < window_start:
            continue
        return DuplicateCheckResult(
            is_duplicate=True,
            matched_event_id=decision.event_id,
            matched_action=decision.action,
            detail=(
                f"Found recent executable action {decision.action} for payment_id={payment_id}, "
                f"attempt={attempt_number}, event_id={decision.event_id} within {dedupe_window_minutes} minutes."
            ),
        )

    return DuplicateCheckResult(
        is_duplicate=False,
        detail="No recent executable action matched payment, attempt, and dedupe window.",
    )
