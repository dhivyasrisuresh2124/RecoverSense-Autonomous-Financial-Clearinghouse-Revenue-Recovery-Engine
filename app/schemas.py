from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class PaymentEventIn(BaseModel):
    event_id: str
    customer_id: str
    payment_id: str
    subscription_id: Optional[str] = None
    amount: float
    currency: str = "INR"
    timestamp: str
    failure_reason: str
    attempt_number: int = 1
    mandate_status: Optional[str] = "ACTIVE"
    payment_status: Optional[str] = "FAILED"
    source: str = "PRODUCTION"
    mode: Optional[str] = None
    simulation_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SimulateEventsRequest(BaseModel):
    n_events: int = 20
    seed: int = 42


class DecisionResponse(BaseModel):
    event_id: str
    payment_id: str
    failure_class: str
    timing: Dict[str, Any]
    ai_context: Dict[str, Any]
    recovery: Dict[str, Any]
    expected_value: Dict[str, Any]
    policy: Dict[str, Any]
    execution: Optional[Dict[str, Any]]
    audit: Dict[str, Any]


class OpportunityOut(BaseModel):
    event_id: str
    payment_id: str
    customer_id: str
    amount: float
    failure_class: str
    timing_score: Optional[float]
    recovery_probability: Optional[float]
    expected_net_recovery: Optional[float]
    action: str
    policy_verdict: str

    class Config:
        from_attributes = True


class EvaluationRunRequest(BaseModel):
    n_dev: int = 3000
    n_test: int = 800
    seed: int = 42


class PolicyConfigIn(BaseModel):
    max_retry_attempts: int = 3
    min_expected_net_recovery: float = 25.0
    communication_window_start_hour: int = 8
    communication_window_end_hour: int = 21
    max_autonomous_amount: float = 50000.0
