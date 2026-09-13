import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import PaymentEventDB
from app.schemas import SimulateEventsRequest
from data.generator import SyntheticDataEngine

router = APIRouter(prefix="/simulate", tags=["simulate"])


@router.post("/events")
def simulate_events(req: SimulateEventsRequest, db: Session = Depends(get_db)):
    """
    Generates N synthetic failure events and persists them as pending
    opportunities (does NOT run the decision pipeline — call
    /decisions/run/{event_id} or /decisions/run-all separately).
    """
    engine = SyntheticDataEngine(seed=req.seed)
    events = engine.generate(req.n_events)

    created = []
    for ev in events:
        existing = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == ev["event_id"]).first()
        if existing:
            continue
        row = PaymentEventDB(
            event_id=ev["event_id"],
            customer_id=ev["customer_id"],
            payment_id=ev["payment_id"],
            subscription_id=ev["subscription_id"],
            amount=ev["amount"],
            currency=ev["currency"],
            timestamp=ev["timestamp"],
            failure_reason=ev["failure_reason"],
            failure_class=ev["failure_class"],
            attempt_number=ev["attempt_number"],
            mandate_status=ev["mandate_status"],
            payment_status=ev["payment_status"],
            metadata_json=ev["metadata"],
        )
        db.add(row)
        created.append(ev["event_id"])
    db.commit()
    return {"created": len(created), "event_ids": created}
