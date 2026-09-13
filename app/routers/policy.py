import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter
from app.schemas import PolicyConfigIn
from policy.policy_engine import PolicyConfig, POLICY_VERSION

router = APIRouter(prefix="/policy", tags=["policy"])

_current_config = PolicyConfig()


@router.get("/config")
def get_policy_config():
    return {
        "policy_version": _current_config.policy_version,
        "max_retry_attempts": _current_config.max_retry_attempts,
        "min_expected_net_recovery": _current_config.min_expected_net_recovery,
        "communication_window_start_hour": _current_config.communication_window_start_hour,
        "communication_window_end_hour": _current_config.communication_window_end_hour,
        "max_autonomous_amount": _current_config.max_autonomous_amount,
    }


@router.put("/config")
def update_policy_config(cfg: PolicyConfigIn):
    global _current_config
    _current_config = PolicyConfig(
        policy_version=POLICY_VERSION,
        max_retry_attempts=cfg.max_retry_attempts,
        min_expected_net_recovery=cfg.min_expected_net_recovery,
        communication_window_start_hour=cfg.communication_window_start_hour,
        communication_window_end_hour=cfg.communication_window_end_hour,
        max_autonomous_amount=cfg.max_autonomous_amount,
    )
    return {"status": "updated", "config": cfg.dict()}
