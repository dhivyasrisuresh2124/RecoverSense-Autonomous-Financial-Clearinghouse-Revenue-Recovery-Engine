"""Explicitly rebuild the local DEMO database from deterministic seed data.

This script is intentionally never called during application startup. It is
guarded against production settings and only accepts the local SQLite demo
database. Run it from the repository root when a clean demo is needed:

    python scripts/reset_demo_database.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from app.config import get_settings
from app.database import SessionLocal, database_url, init_db
from app.models_db import AuditRecordDB, DecisionDB, IdempotencyKeyDB, LedgerHeadDB, PaymentEventDB
from audit.audit_ledger import GENESIS_HASH
from data.generator import SyntheticDataEngine


def reset_demo_database(n_events: int = 6, seed: int = 42) -> dict:
    settings = get_settings()
    if settings.PRODUCTION_MODE or not settings.DEMO_MODE:
        raise RuntimeError("Refusing to reset a non-DEMO database.")
    if not settings.DATABASE_URL.startswith("sqlite:///"):
        raise RuntimeError("Demo reset only supports the local SQLite database.")
    database_path = os.path.abspath(database_url[len("sqlite:///"):])
    expected_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend", "recoversense.db"))
    if database_path != expected_path:
        raise RuntimeError("Demo reset is restricted to backend/recoversense.db.")

    init_db()
    db = SessionLocal()
    try:
        for model in (IdempotencyKeyDB, AuditRecordDB, DecisionDB, PaymentEventDB):
            db.query(model).delete(synchronize_session=False)
        head = db.get(LedgerHeadDB, 1)
        if head is None:
            head = LedgerHeadDB(id=1)
            db.add(head)
        head.seq = -1
        head.current_hash = GENESIS_HASH
        db.commit()

        engine = SyntheticDataEngine(seed=seed)
        events = engine.generate(n_events)
        from app.routers.decisions import load_or_train_demo_model, run_single_decision

        if not load_or_train_demo_model():
            raise RuntimeError("Could not load or train the demo recovery model.")

        for event in events:
            db.add(PaymentEventDB(
                event_id=event["event_id"], customer_id=event["customer_id"],
                payment_id=event["payment_id"], subscription_id=event["subscription_id"],
                amount=event["amount"], currency=event["currency"],
                timestamp=event["timestamp"], failure_reason=event["failure_reason"],
                failure_class=event["failure_class"], attempt_number=event["attempt_number"],
                mandate_status=event["mandate_status"], payment_status=event["payment_status"],
                source="SIMULATOR", mode="SIMULATION", simulation_id=f"RESET-{seed}",
                metadata_json=event["metadata"],
            ))
        db.commit()

        for event in events:
            run_single_decision(event["event_id"], db)

        return {"events_seeded": len(events), "audit_records_seeded": db.query(AuditRecordDB).count()}
    finally:
        db.close()


if __name__ == "__main__":
    print(reset_demo_database())
