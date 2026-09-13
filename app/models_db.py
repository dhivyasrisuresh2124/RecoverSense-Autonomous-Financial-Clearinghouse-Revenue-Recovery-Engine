from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, UniqueConstraint
from sqlalchemy.sql import func
from app.database import Base


class PaymentEventDB(Base):
    __tablename__ = "payment_events"
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    customer_id = Column(String, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    subscription_id = Column(String)
    amount = Column(Float, nullable=False)
    currency = Column(String, default="INR")
    timestamp = Column(String, nullable=False)
    failure_reason = Column(String, nullable=False)
    failure_class = Column(String)
    attempt_number = Column(Integer, default=1, nullable=False)
    mandate_status = Column(String)
    payment_status = Column(String)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    metadata_json = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())


class DecisionDB(Base):
    __tablename__ = "decisions"
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    customer_id = Column(String, index=True, nullable=False)
    amount = Column(Float, nullable=False)
    attempt_number = Column(Integer, default=1, nullable=False)
    failure_class = Column(String)
    timing_score = Column(Float, nullable=True)
    recommended_window = Column(String, nullable=True)
    recovery_probability = Column(Float, nullable=True)
    expected_net_recovery = Column(Float, nullable=True)
    expected_recovery = Column(Float, nullable=True)
    expected_utility = Column(Float, nullable=True)
    selected_channel = Column(String, nullable=True)
    nerv = Column(Float, nullable=True)
    intervention_cost = Column(Float, nullable=True)
    selected_strategy = Column(String, nullable=True, index=True)
    alternatives_json = Column(JSON, nullable=True)
    portfolio_rank = Column(Integer, nullable=True, index=True)
    optimization_cycle_id = Column(String, nullable=True, index=True)
    ai_recommendation_json = Column(JSON)
    policy_verdict = Column(String)
    policy_checks_json = Column(JSON)
    action = Column(String)
    execution_result_json = Column(JSON, nullable=True)
    outcome = Column(String, nullable=True)
    actual_recovered_amount = Column(Float, nullable=True)
    outcome_timestamp = Column(DateTime, nullable=True)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    model_version = Column(String)
    policy_version = Column(String)
    audit_seq = Column(Integer, nullable=True)
    audit_hash = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class AuditRecordDB(Base):
    __tablename__ = "audit_records"
    id = Column(Integer, primary_key=True, index=True)
    seq = Column(Integer, unique=True, index=True, nullable=False)
    event_id = Column(String, index=True)
    payment_id = Column(String, index=True)
    attempt_number = Column(Integer, default=1, nullable=False)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    timestamp = Column(Float)
    failure_class = Column(String)
    timing_score = Column(Float, nullable=True)
    recommended_window = Column(String, nullable=True)
    recovery_probability = Column(Float, nullable=True)
    expected_net_recovery = Column(Float, nullable=True)
    ai_recommendation_json = Column(JSON)
    policy_verdict = Column(String)
    policy_checks_json = Column(JSON)
    action = Column(String)
    execution_result_json = Column(JSON, nullable=True)
    outcome = Column(String, nullable=True)
    selected_strategy = Column(String, nullable=True)
    expected_recovery = Column(Float, nullable=True)
    expected_utility = Column(Float, nullable=True)
    portfolio_rank = Column(Integer, nullable=True)
    actual_recovered_amount = Column(Float, nullable=True)
    # Pillar 4: realized utility from the observed outcome + the learning
    # update payload (data_kind, counterfactuals) for the audit chain.
    realized_utility = Column(Float, nullable=True)
    selected_channel = Column(String, nullable=True)
    nerv = Column(Float, nullable=True)
    intervention_cost = Column(Float, nullable=True)
    learning_update_json = Column(JSON, nullable=True)
    model_version = Column(String)
    policy_version = Column(String)
    prev_hash = Column(String)
    current_hash = Column(String)


class LedgerHeadDB(Base):
    __tablename__ = "audit_ledger_head"
    id = Column(Integer, primary_key=True)
    seq = Column(Integer, nullable=False, default=-1)
    current_hash = Column(String, nullable=False)


class EvaluationResultDB(Base):
    """Latest reproducible synthetic benchmark, separate from operations."""
    __tablename__ = "evaluation_results"
    id = Column(Integer, primary_key=True)
    result_json = Column(JSON, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ProviderOutcomeEvidenceDB(Base):
    """Idempotent, provider-verified capture evidence.

    Evidence is intentionally separate from a RecoverSense decision outcome:
    an unmatched provider payment must never inflate recovered revenue.
    """
    __tablename__ = "provider_outcome_evidence"
    id = Column(Integer, primary_key=True, index=True)
    provider_event_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, unique=True, index=True, nullable=False)
    event_type = Column(String, nullable=False)
    provider_status = Column(String, nullable=False)
    reconciliation_status = Column(String, nullable=False)
    matched_event_id = Column(String, nullable=True, index=True)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=True)
    order_id = Column(String, nullable=True)
    invoice_id = Column(String, nullable=True)
    subscription_id = Column(String, nullable=True)
    method = Column(String, nullable=True)
    evidence_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class ProviderRefundDB(Base):
    """Idempotent provider refund facts, separate from the AI decision."""
    __tablename__ = "provider_refunds"
    id = Column(Integer, primary_key=True, index=True)
    refund_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    event_type = Column(String, nullable=False)
    provider_status = Column(String, nullable=False)
    matched_event_id = Column(String, nullable=True, index=True)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=True)
    evidence_json = Column(JSON, nullable=True)
    audit_seq = Column(Integer, nullable=True, unique=True)
    audit_hash = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class IdempotencyKeyDB(Base):
    __tablename__ = "idempotency_keys"
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class StrategyPerformanceDB(Base):
    __tablename__ = "strategy_performance"
    id = Column(Integer, primary_key=True, index=True)
    strategy = Column(String, nullable=False, index=True)
    context_key = Column(String, nullable=False, index=True)
    attempts = Column(Integer, default=0, nullable=False)
    successful_recoveries = Column(Integer, default=0, nullable=False)
    expected_recovery_total = Column(Float, default=0.0, nullable=False)
    actual_recovered_total = Column(Float, default=0.0, nullable=False)
    # Pillar 4: cumulative realized utility from OBSERVED outcomes only
    # (revenue recovered - provider fee - intervention cost - friction loss).
    realized_utility_total = Column(Float, default=0.0, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class LearningUpdateDB(Base):
    """Persistent closed-loop learning update (Pillar 4).

    Every row is explicitly labelled:
      * OBSERVED_OUTCOME        — a real, observable economic outcome
                                  (realized_utility is REAL).
      * ESTIMATED_COUNTERFACTUAL — a strategy that was NOT executed; the
                                  value is a model estimate and is never
                                  treated as realized/actual recovery.
    """
    __tablename__ = "learning_updates"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    strategy = Column(String, index=True, nullable=False)
    context_key = Column(String, index=True, nullable=False)
    data_kind = Column(String, nullable=False, index=True)
    outcome = Column(String, nullable=True)
    expected_utility = Column(Float, default=0.0, nullable=False)
    actual_recovered = Column(Float, default=0.0, nullable=False)
    realized_utility = Column(Float, default=0.0, nullable=False)
    counterfactuals_json = Column(JSON, nullable=True)
    audit_seq = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class OptimizationCycleDB(Base):
    __tablename__ = "optimization_cycles"
    id = Column(String, primary_key=True)
    # SQLite timestamps have second precision; this is the authoritative order.
    sequence = Column(Integer, unique=True, index=True, nullable=True)
    max_actions = Column(Integer, nullable=False)
    revenue_at_risk = Column(Float, nullable=False)
    expected_recoverable = Column(Float, nullable=False)
    expected_selected_recovery = Column(Float, nullable=False)
    expected_nerv = Column(Float, default=0.0, nullable=False)
    intervention_spend = Column(Float, default=0.0, nullable=False)
    actions_recommended = Column(Integer, nullable=False)
    actions_authorized = Column(Integer, default=0, nullable=False)
    actions_blocked = Column(Integer, default=0, nullable=False)
    actual_recovered = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class OptimizationCycleSequenceDB(Base):
    __tablename__ = "optimization_cycle_sequence"
    id = Column(Integer, primary_key=True)
    current_sequence = Column(Integer, nullable=False, default=0)


class PortfolioOpportunityDB(Base):
    """Optimizer record, intentionally distinct from an execution DecisionDB."""
    __tablename__ = "portfolio_opportunities"
    __table_args__ = (UniqueConstraint("optimization_cycle_id", "event_id", name="uq_cycle_event"),)
    id = Column(Integer, primary_key=True, index=True)
    optimization_cycle_id = Column(String, nullable=False, index=True)
    event_id = Column(String, nullable=False, index=True)
    payment_id = Column(String, nullable=False, index=True)
    customer_id = Column(String, nullable=True, index=True)
    amount = Column(Float, nullable=False)
    recovery_probability = Column(Float, nullable=False)
    expected_recovery = Column(Float, nullable=False)
    expected_utility = Column(Float, nullable=False)
    selected_channel = Column(String, nullable=True)
    nerv = Column(Float, nullable=True)
    intervention_cost = Column(Float, nullable=True)
    portfolio_rank = Column(Integer, nullable=False, index=True)
    selected_strategy = Column(String, nullable=False, index=True)
    alternatives_json = Column(JSON, nullable=False)
    allocation_status = Column(String, nullable=False, index=True)
    policy_verdict = Column(String, nullable=True)
    policy_checks_json = Column(JSON, nullable=True)
    explanation_json = Column(JSON, nullable=False)
    execution_status = Column(String, nullable=True)
    execution_result_json = Column(JSON, nullable=True)
    outcome = Column(String, nullable=True)
    actual_recovered_amount = Column(Float, nullable=True)
    optimizer_version = Column(String, nullable=False)
    strategy_version = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class FlashSettleExposureDB(Base):
    """Persistent record of a Flash-Settle exposure (Pillar 1).

    Internal financial decision/ledger representation only — this does NOT
    represent an external Razorpay money-transfer API call. The advance is a
    controlled merchant-side recovery advance tracked internally.
    """
    __tablename__ = "flash_settle_exposures"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    customer_id = Column(String, index=True, nullable=False)
    merchant_id = Column(String, index=True, nullable=False, default="default")

    # Underwriting outputs (snapshotted at authorization time).
    recovery_probability = Column(Float, nullable=False)
    recovery_horizon_hours = Column(Float, nullable=False)
    proposed_advance_amount = Column(Float, nullable=False, default=0.0)
    merchant_exposure = Column(Float, nullable=False)
    merchant_reserve_cap = Column(Float, nullable=False)

    # Financial state.
    advance_amount = Column(Float, nullable=False, default=0.0)
    recovered_amount = Column(Float, nullable=False, default=0.0)
    outstanding_amount = Column(Float, nullable=False, default=0.0)

    # Lifecycle state: AUTHORIZED | ADVANCE_RECORDED | AWAITING_RECOVERY |
    # RECOVERY_OBSERVED | RECONCILED | EXPIRED | DEFERRED.
    state = Column(String, nullable=False, default="AUTHORIZED", index=True)

    # Safety / audit.
    idempotency_key = Column(String, unique=True, index=True)
    underwriting_result_json = Column(JSON, nullable=True)
    policy_checks_json = Column(JSON, nullable=True)
    provider_verification_json = Column(JSON, nullable=True)
    reconciliation_outcome_json = Column(JSON, nullable=True)
    audit_seq = Column(Integer, nullable=True)
    audit_hash = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)

    # Timestamps.
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class YieldCurveDecisionDB(Base):
    """Persistent record of a Yield Curve partial-recovery decision (Pillar 2).

    Internal decision/ledger representation only. The current Razorpay
    integration has no documented partial-amount charge write operation, so a
    Yield Curve authorization is recorded as ``INTERNAL_LEDGER_ONLY`` with
    capability ``UNSUPPORTED_PROVIDER_OPERATION`` — no external API call is
    implied, faked, or claimed.
    """
    __tablename__ = "yield_curve_decisions"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    customer_id = Column(String, index=True, nullable=False)

    # Underwriting outputs (snapshotted at authorization time).
    original_amount = Column(Float, nullable=False)
    partial_level = Column(Float, nullable=False)
    partial_amount = Column(Float, nullable=False, default=0.0)
    remaining_balance = Column(Float, nullable=False, default=0.0)
    base_recovery_probability = Column(Float, nullable=False)
    partial_recovery_probability = Column(Float, nullable=False)
    expected_utility = Column(Float, nullable=False, default=0.0)

    # Financial state.
    observed_partial_recovery = Column(Float, nullable=False, default=0.0)
    outstanding_balance = Column(Float, nullable=False, default=0.0)

    # Lifecycle state: YIELD_CURVE_EVALUATED | PARTIAL_RECOVERY_AUTHORIZED |
    # PARTIAL_RECOVERY_RECORDED | REMAINING_BALANCE_SCHEDULED |
    # PARTIAL_RECOVERY_OBSERVED | COMPLETED | DEFERRED | EXPIRED.
    state = Column(String, nullable=False,
                   default="PARTIAL_RECOVERY_AUTHORIZED", index=True)

    # Safety / audit.
    idempotency_key = Column(String, unique=True, index=True)
    underwriting_result_json = Column(JSON, nullable=True)
    policy_checks_json = Column(JSON, nullable=True)
    provider_verification_json = Column(JSON, nullable=True)
    reconciliation_outcome_json = Column(JSON, nullable=True)
    scheduled_windows_json = Column(JSON, nullable=True)
    audit_seq = Column(Integer, nullable=True)
    audit_hash = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)

    # Timestamps.
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class MultiRailDecisionDB(Base):
    """Persistent record of a Multi-Rail intervention decision (Pillar 3).

    Internal decision/ledger representation only. The current Razorpay
    integration has no documented operation to create/change a mandate or
    switch a recurring payment's rail, so an authorized rail switch is
    recorded as ``INTERNAL_LEDGER_ONLY`` with capability
    ``UNSUPPORTED_PROVIDER_OPERATION`` — no external API call is implied,
    faked, or claimed.
    """
    __tablename__ = "multi_rail_decisions"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    payment_id = Column(String, index=True, nullable=False)
    customer_id = Column(String, index=True, nullable=False)

    # Rail transition.
    source_rail = Column(String, nullable=False, index=True)
    target_rail = Column(String, nullable=False, index=True)

    # Underwriting outputs (snapshotted at authorization time).
    original_amount = Column(Float, nullable=False)
    base_recovery_probability = Column(Float, nullable=False)
    target_recovery_probability = Column(Float, nullable=False)
    expected_utility = Column(Float, nullable=False, default=0.0)

    # Financial state.
    observed_recovery = Column(Float, nullable=False, default=0.0)
    outstanding_amount = Column(Float, nullable=False, default=0.0)

    # Lifecycle state: MULTI_RAIL_EVALUATED | RAIL_SWITCH_AUTHORIZED |
    # RAIL_SWITCH_RECORDED | AWAITING_RAIL_OUTCOME | RAIL_OUTCOME_OBSERVED |
    # COMPLETED | DEFERRED | EXPIRED.
    state = Column(String, nullable=False,
                   default="RAIL_SWITCH_AUTHORIZED", index=True)

    # Safety / audit.
    idempotency_key = Column(String, unique=True, index=True)
    underwriting_result_json = Column(JSON, nullable=True)
    policy_checks_json = Column(JSON, nullable=True)
    provider_verification_json = Column(JSON, nullable=True)
    reconciliation_outcome_json = Column(JSON, nullable=True)
    audit_seq = Column(Integer, nullable=True)
    audit_hash = Column(String, nullable=True)
    simulation_id = Column(String, nullable=True, index=True)
    source = Column(String, default="PRODUCTION", nullable=False)
    mode = Column(String, nullable=True)

    # Timestamps.
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


