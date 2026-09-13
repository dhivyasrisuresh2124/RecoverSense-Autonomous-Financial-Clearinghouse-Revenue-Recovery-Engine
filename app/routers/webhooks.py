import hashlib
import hmac
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, BackgroundTasks, Depends, Request, HTTPException
from starlette.concurrency import run_in_threadpool
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models_db import DecisionDB, PaymentEventDB, ProviderOutcomeEvidenceDB, ProviderRefundDB
from app.config import get_settings
from agent.runtime import process_event_through_pipeline
from models.failure_classifier import classify_failure
from execution.razorpay_client import RazorpayTestModeAdapter

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = logging.getLogger("recoversense.webhooks")


def verify_signature(payload_body: bytes, signature: str, secret: str, demo_mode: bool) -> bool:
    """
    FAIL-CLOSED signature verification.

    If a webhook secret IS configured, verification always runs and an
    invalid/missing signature is rejected — regardless of DEMO_MODE.

    If NO secret is configured: this is only tolerated when DEMO_MODE is
    true (the default, intended for local/buildathon use without a real
    Razorpay webhook secret). When DEMO_MODE is false (i.e. someone has
    explicitly turned it off) and no secret is configured, verification
    fails closed — an unsigned webhook is rejected outright rather than
    silently accepted. This mirrors the "PRODUCTION_MODE=true requires
    RAZORPAY_WEBHOOK_SECRET or startup fails" check in app/main.py.
    """
    if not secret:
        return bool(demo_mode)  # explicit, narrow exception — not a silent bypass
    expected = hmac.new(secret.encode(), payload_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


def _ingest_razorpay_event(db: Session, entity: dict, event_type: str) -> dict:
    """Persist a normalized payment-failure event; idempotent per event_id.

    Kept as its own synchronous function so the route can execute it in the
    threadpool instead of on the event loop.
    """
    event_id = f"razorpay_{entity.get('id')}_{event_type}"
    existing = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == event_id).first()
    if existing:
        logger.info("webhook_duplicate event_id=%s event_type=%s", event_id, event_type)
        return {"status": "duplicate_ignored", "event_id": event_id}

    # A failure event may still omit error_reason; never invoke string methods
    # on nullable provider fields.
    failure_reason = str(entity.get("error_reason") or "UNKNOWN_FAILURE").upper()
    failure_class = classify_failure(failure_reason)

    row = PaymentEventDB(
        event_id=event_id,
        customer_id=entity.get("customer_id", "unknown_customer"),
        payment_id=entity.get("id"),
        subscription_id=entity.get("subscription_id"),
        amount=(entity.get("amount", 0) or 0) / 100.0,  # Razorpay amounts are in paise
        currency=entity.get("currency", "INR"),
        timestamp=str(entity.get("created_at", "")),
        failure_reason=failure_reason,
        failure_class=failure_class,
        attempt_number=1,
        mandate_status="ACTIVE",
        payment_status="FAILED",
        metadata_json={"raw_webhook_event": event_type, "support_note": ""},
    )
    try:
        with db.begin_nested():
            db.add(row)
            db.flush()
        db.commit()
    except IntegrityError:
        db.rollback()
        return {"status": "duplicate_ignored", "event_id": event_id}

    return {"status": "ingested", "event_id": event_id, "failure_class": failure_class}


def _provider_ids(entity: dict) -> dict:
    """Extract only the correlation fields needed for reconciliation."""
    return {
        "payment_id": entity.get("id"), "order_id": entity.get("order_id"),
        "invoice_id": entity.get("invoice_id"), "subscription_id": entity.get("subscription_id"),
        "method": entity.get("method"),
    }


def _matches_provider_relationship(event: PaymentEventDB, decision: DecisionDB, ids: dict) -> bool:
    """Match exact provider identifiers only; amount is never a key."""
    if ids["payment_id"] and decision.payment_id == ids["payment_id"]:
        return True
    metadata = event.metadata_json or {}
    payment_entity = (metadata.get("payment_entity") or metadata.get("raw_payment_entity")
                      or metadata.get("entity") or {})
    persisted_subscription_id = (event.subscription_id or metadata.get("subscription_id")
                                 or payment_entity.get("subscription_id"))
    if ids["subscription_id"] and persisted_subscription_id == ids["subscription_id"]:
        return True
    execution = decision.execution_result_json or {}
    for field in ("order_id", "invoice_id", "subscription_id"):
        value = ids[field]
        if value and (metadata.get(field) == value or execution.get(f"provider_{field}") == value):
            return True
    return False


def _find_related_decision(db: Session, ids: dict):
    """Return a causally related decision, never one selected by amount."""
    rows = db.query(DecisionDB, PaymentEventDB).join(
        PaymentEventDB, PaymentEventDB.event_id == DecisionDB.event_id
    ).all()
    for decision, event in rows:
        if _matches_provider_relationship(event, decision, ids):
            return decision, event
    return None, None


def _store_provider_evidence(db: Session, *, event_type: str, ids: dict, provider_state: dict,
                             reconciliation_status: str, matched_event_id: str | None = None) -> dict:
    """Upsert one payment-level provider fact across Razorpay event types.

    Razorpay legitimately emits more than one event (for example
    ``payment.captured`` and ``order.paid``) for a captured payment.  The
    database intentionally has one evidence row per payment, so later
    deliveries merge their safe evidence rather than attempting another row.
    """
    payment_id = ids["payment_id"]
    provider_event_id = f"razorpay_{payment_id}_{event_type}"
    safe_state = {key: provider_state.get(key) for key in (
        "id", "status", "amount", "currency", "order_id", "invoice_id", "subscription_id", "method"
    ) if provider_state.get(key) is not None}

    def merge(existing: ProviderOutcomeEvidenceDB) -> dict:
        # Never downgrade a causally reconciled recovery merely because a
        # duplicate delivery carries a different event wrapper/state.
        prior_evidence = existing.evidence_json or {}
        event_types = set(prior_evidence.get("webhook_event_types", []))
        event_types.add(existing.event_type)
        event_types.add(event_type)
        existing.evidence_json = {**prior_evidence, **safe_state,
                                  "webhook_event_types": sorted(event_types)}
        return {"status": "duplicate_ignored", "payment_id": payment_id,
                "reconciliation_status": existing.reconciliation_status,
                "matched_event_id": existing.matched_event_id}

    existing = db.query(ProviderOutcomeEvidenceDB).filter(
        ProviderOutcomeEvidenceDB.payment_id == payment_id
    ).first()
    if existing:
        return merge(existing)
    row = ProviderOutcomeEvidenceDB(
        provider_event_id=provider_event_id, payment_id=payment_id, event_type=event_type,
        provider_status=str(provider_state.get("status") or "UNAVAILABLE").upper(),
        reconciliation_status=reconciliation_status, matched_event_id=matched_event_id,
        amount=(provider_state.get("amount", 0) or 0) / 100.0,
        currency=provider_state.get("currency"), order_id=ids["order_id"], invoice_id=ids["invoice_id"],
        subscription_id=ids["subscription_id"], method=ids["method"], evidence_json=safe_state,
    )
    try:
        # A concurrent legitimate webhook can pass the read above.  Contain
        # the unique-key conflict to this savepoint, then merge the winner.
        with db.begin_nested():
            db.add(row)
            db.flush()
    except IntegrityError:
        existing = db.query(ProviderOutcomeEvidenceDB).filter(
            ProviderOutcomeEvidenceDB.payment_id == payment_id
        ).first()
        if existing:
            return merge(existing)
        raise
    return {"status": reconciliation_status, "payment_id": payment_id, "matched_event_id": matched_event_id}


def _reconcile_captured_payment(db: Session, entity: dict, event_type: str,
                                provider_adapter=None) -> dict:
    """Fresh-verify a provider capture, then reconcile only exact relationships."""
    ids = _provider_ids(entity)
    if not ids["payment_id"]:
        return {"status": "rejected", "reason": "missing_payment_id"}
    existing = db.query(ProviderOutcomeEvidenceDB).filter(
        ProviderOutcomeEvidenceDB.payment_id == ids["payment_id"]
    ).first()
    if existing:
        # A second Razorpay event for this same payment is acknowledged as a
        # legitimate duplicate while preserving a single payment-level fact.
        # Its event type is merged for traceability; no decision/outcome path
        # runs again and no provider revenue is counted twice.
        result = _store_provider_evidence(
            db, event_type=event_type, ids=ids, provider_state=entity,
            reconciliation_status=existing.reconciliation_status,
            matched_event_id=existing.matched_event_id,
        )
        db.commit()
        return result

    provider_state = (provider_adapter or RazorpayTestModeAdapter()).get_payment_state(ids["payment_id"])
    if (str(provider_state.get("status") or "").lower() != "captured"
            or provider_state.get("id") != ids["payment_id"]):
        result = _store_provider_evidence(
            db, event_type=event_type, ids=ids, provider_state=provider_state,
            reconciliation_status="PROVIDER_REJECTED",
        )
        db.commit()
        return result

    decision, event = _find_related_decision(db, ids)
    if not decision:
        result = _store_provider_evidence(
            db, event_type=event_type, ids=ids, provider_state=provider_state,
            reconciliation_status="NO_MATCH_PROVIDER_EVIDENCE",
        )
        db.commit()
        return result
    amount = (provider_state.get("amount", 0) or 0) / 100.0
    if amount > decision.amount or decision.outcome:
        result = _store_provider_evidence(
            db, event_type=event_type, ids=ids, provider_state=provider_state,
            reconciliation_status="PROVIDER_REJECTED", matched_event_id=decision.event_id,
        )
        db.commit()
        return result

    # Existing outcome path updates learning, portfolio totals, and appends an
    # immutable audit fact. It is invoked only after fresh captured state and
    # exact provider correlation have both been established.
    from app.routers.optimization import OutcomeIn, record_outcome
    outcome = record_outcome(decision.event_id, OutcomeIn(outcome="RECOVERED", actual_recovered_amount=amount), db)
    _store_provider_evidence(db, event_type=event_type, ids=ids, provider_state=provider_state,
                             reconciliation_status="RECOVERED", matched_event_id=decision.event_id)
    db.commit()
    return {"status": "RECOVERED", "payment_id": ids["payment_id"], "matched_event_id": decision.event_id,
            "outcome": outcome}


def _reconcile_refund(db: Session, entity: dict, event_type: str) -> dict:
    """Record a signed refund fact without creating an opportunity or altering a decision."""
    refund_id, payment_id = entity.get("id"), entity.get("payment_id")
    if not refund_id or not payment_id:
        return {"status": "rejected", "reason": "missing_refund_or_payment_id"}
    refund = db.query(ProviderRefundDB).filter(ProviderRefundDB.refund_id == refund_id).first()
    status = str(entity.get("status") or "created").upper()
    amount = (entity.get("amount", 0) or 0) / 100.0
    evidence = {key: entity.get(key) for key in ("id", "payment_id", "amount", "currency", "status")
                if entity.get(key) is not None}
    matched = db.query(ProviderOutcomeEvidenceDB).filter(
        ProviderOutcomeEvidenceDB.payment_id == payment_id,
        ProviderOutcomeEvidenceDB.reconciliation_status == "RECOVERED",
    ).first()
    if refund:
        # created -> processed is a state transition for the same provider refund,
        # not a second financial reversal.
        if refund.provider_status == "PROCESSED" or refund.provider_status == status:
            return {"status": "duplicate_ignored", "refund_id": refund_id}
        refund.provider_status, refund.event_type, refund.evidence_json = status, event_type, evidence
        if matched and matched.matched_event_id:
            refund.matched_event_id = matched.matched_event_id
    else:
        refund = ProviderRefundDB(refund_id=refund_id, payment_id=payment_id, event_type=event_type,
            provider_status=status, matched_event_id=(matched.matched_event_id if matched else None),
            amount=amount, currency=entity.get("currency"), evidence_json=evidence)
        db.add(refund)
    # A processed, matched reversal appends an audit fact. It does not change
    # DecisionDB's original outcome or re-enter the learning pipeline.
    if status == "PROCESSED" and refund.matched_event_id and refund.audit_seq is None:
        decision = db.query(DecisionDB).filter(DecisionDB.event_id == refund.matched_event_id).first()
        event = db.query(PaymentEventDB).filter(PaymentEventDB.event_id == refund.matched_event_id).first()
        if decision and event:
            from app.routers.decisions import _db_ledger_append
            audit = _db_ledger_append(db, event_id=event.event_id, payment_id=payment_id,
                attempt_number=decision.attempt_number, source=decision.source, mode=decision.mode,
                simulation_id=decision.simulation_id, failure_class=decision.failure_class,
                timing_score=decision.timing_score, recommended_window=decision.recommended_window,
                recovery_probability=decision.recovery_probability, expected_net_recovery=decision.expected_net_recovery,
                ai_recommendation=decision.ai_recommendation_json or {}, policy_verdict=decision.policy_verdict,
                policy_checks=decision.policy_checks_json or [], action="REFUND_RECORDED",
                execution_result={"provider": "razorpay", "refund_id": refund_id, "amount": amount, "status": status},
                outcome="REFUNDED", model_version=decision.model_version or "unknown",
                policy_version=decision.policy_version or "unknown", selected_strategy=decision.selected_strategy,
                expected_recovery=decision.expected_recovery, expected_utility=decision.expected_utility,
                portfolio_rank=decision.portfolio_rank, actual_recovered_amount=0.0,
                selected_channel=decision.selected_channel, nerv=decision.nerv,
                intervention_cost=decision.intervention_cost)
            refund.audit_seq, refund.audit_hash = audit.seq, audit.current_hash
    db.commit()
    return {"status": "REFUND_PROCESSED" if status == "PROCESSED" else "REFUND_RECORDED",
            "refund_id": refund_id, "matched_event_id": refund.matched_event_id}


@router.post("/razorpay")
async def razorpay_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Accepts signed Razorpay payment outcome webhooks. Failure events normalize
    into PaymentEventDB and trigger the existing decision runtime; captured
    events use a fresh provider read and reconcile without re-entering it.

    Supported event types: payment.failed, subscription.charged.failed,
    payment.captured, subscription.charged, and order.paid when it contains
    the payment entity needed for fresh verification.
    See: https://razorpay.com/docs/webhooks/payloads/payments/
    """
    settings = get_settings()
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    if not verify_signature(body, signature, settings.RAZORPAY_WEBHOOK_SECRET, settings.DEMO_MODE):
        logger.warning(
            "webhook_rejected reason=invalid_signature remote=%s",
            request.client.host if request.client else "unknown",
        )
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    payload = await request.json()
    event_type = payload.get("event", "unknown")

    if event_type not in {
        "payment.failed", "subscription.charged.failed", "payment.captured",
        "subscription.charged", "order.paid", "refund.created", "refund.processed",
    }:
        # Signed subscription lifecycle notifications do not represent a
        # failure or a reconciled outcome. Acknowledge them without DB writes.
        logger.info("webhook_ignored event_type=%s", event_type)
        return {"status": "ignored", "event_type": event_type}

    try:
        entity = payload["payload"]["refund" if event_type.startswith("refund.") else "payment"]["entity"]
    except (KeyError, TypeError):
        logger.warning("webhook_rejected reason=unrecognized_payload_shape event_type=%s", event_type)
        raise HTTPException(status_code=422, detail="Unrecognized webhook payload shape")

    if event_type in {"payment.captured", "subscription.charged", "order.paid"}:
        # A capture is outcome evidence, never a new failure for the decision
        # pipeline. Reconciliation fresh-fetches the payment and requires an
        # exact provider relationship before recording recovered revenue.
        return await run_in_threadpool(_reconcile_captured_payment, db, entity, event_type)

    if event_type.startswith("refund."):
        return await run_in_threadpool(_reconcile_refund, db, entity, event_type)

    # Run the synchronous DB work off the event loop: a contended write (e.g.
    # while a background agent lifecycle commits) must never freeze the whole
    # server. Behavior is unchanged — only the execution thread moves.
    ingest_result = await run_in_threadpool(_ingest_razorpay_event, db, entity, event_type)

    if ingest_result["status"] != "ingested":
        return ingest_result

    # Automatically launch the autonomous agent runtime lifecycle in the background
    background_tasks.add_task(process_event_through_pipeline, ingest_result["event_id"])
    logger.info(
        "webhook_accepted event_id=%s event_type=%s failure_class=%s agent_triggered=true",
        ingest_result["event_id"],
        event_type,
        ingest_result["failure_class"],
    )

    return ingest_result
