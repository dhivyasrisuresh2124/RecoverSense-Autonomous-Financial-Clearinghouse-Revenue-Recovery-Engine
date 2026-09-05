"""Provision the production recovery-model artifact deterministically.

PRODUCTION_MODE requires a trained artifact at RECOVERSENSE_MODEL_PATH
(default: backend/artifacts/recovery_model.pkl) or startup fails closed.
This script regenerates that artifact with the *currently installed*
scikit-learn, so the pickle can never be stale relative to the runtime
(the classic sklearn InconsistentVersionWarning deployment failure).

Determinism: the exact same seeded development set (SyntheticDataEngine
seed=42) and training path used by the demo bootstrap (`train_model`) are
reused — no new ML, no new features, no hyperparameters changed.

Usage:
    python scripts/provision_production_model.py [output_path]

`output_path` defaults to the resolved RECOVERSENSE_MODEL_PATH setting.
"""

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The `app`/`models`/`evaluation` packages live in <repo>/backend.
sys.path.insert(0, os.path.join(_REPO_ROOT, "backend"))

from app.config import get_settings  # noqa: E402
from data.generator import SyntheticDataEngine  # noqa: E402
from evaluation.evaluate import train_model  # noqa: E402
from models.recovery_model import RecoveryModel  # noqa: E402

DETERMINISTIC_SEED = 42
DETERMINISTIC_TRAINING_ROWS = 3000


def provision(output_path: str | None = None) -> str:
    """Train on the seeded dev set and save the artifact to output_path."""
    settings = get_settings()
    target = output_path or settings.resolved_model_path

    engine = SyntheticDataEngine(seed=DETERMINISTIC_SEED)
    events = engine.generate(DETERMINISTIC_TRAINING_ROWS)
    model = train_model(events, engine)
    if not model.trained:
        raise RuntimeError("Training did not produce a usable model (is scikit-learn installed?).")

    parent = os.path.dirname(os.path.abspath(target))
    os.makedirs(parent, exist_ok=True)
    model.save(target)

    # Verify the artifact round-trips through the runtime's own loader and
    # fingerprint it so deployments can pin the exact bytes they expect.
    reloaded = RecoveryModel().load(target)
    if not reloaded.trained:
        raise RuntimeError(f"Provisioned artifact at {target} failed to load as trained.")
    print(f"provisioned artifact path={target} fingerprint={reloaded.artifact_fingerprint()}")
    return target


if __name__ == "__main__":
    provision(sys.argv[1] if len(sys.argv) > 1 else None)