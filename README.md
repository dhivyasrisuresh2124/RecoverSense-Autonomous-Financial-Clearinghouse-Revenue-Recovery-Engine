# RecoverSense v2.0

**Closed-loop revenue recovery optimization for failed recurring payments.**

Built for Razorpay AI Buildathon — Track 03: Explainable AI for Financial Decisioning.

RecoverSense does not blindly retry failed payments; it decides where recovery effort is most valuable, chooses the safest effective strategy, executes within policy, measures the money actually recovered, and uses those outcomes to improve future recovery decisions.

## v2 closed-loop optimization

For each failed payment, RecoverSense keeps recovery probability, expected recovery (`amount × probability`), and expected utility (expected recovery less intervention and friction costs) separate. It then evaluates explicit strategies — `RETRY_NOW`, `RETRY_OPTIMAL_WINDOW`, `DEFER_AND_RETRY`, `ESCALATE`, and `STOP` — ranks the eligible opportunities by deterministic expected utility, and allocates the merchant's configurable `max_actions_per_cycle` budget to the highest-value actions.

The policy engine remains a hard boundary: optimization recommends; deterministic policy authorizes; the existing execution adapter performs only its clearly labelled supported/simulated action. Outcome API records (`RECOVERED`, `FAILED`, `DUPLICATE_PREVENTED`, `TIMEOUT`, `DEFERRED`, `ESCALATED`, `UNRESOLVED`) update smoothed per-strategy performance after a minimum evidence threshold, and outcome facts are appended to the tamper-evident audit ledger.

Every event considered by a cycle is persisted in `portfolio_opportunities`, independently of the execution-only `decisions` table. This preserves the distinction between `ACTION_SELECTED`, `DEFERRED`, `UNALLOCATED`, `ESCALATED`, `STOPPED`, and `POLICY_BLOCKED`, including when no provider call occurs.

New API endpoints:

- `POST /optimization/cycles?max_actions_per_cycle=10` — analyze and allocate a pending portfolio.
- `GET /optimization/portfolio` — latest allocated portfolio.
- `POST /optimization/outcomes/{event_id}` — record an actual recovery outcome.
- `GET /optimization/strategy-performance` — evidence-backed strategy metrics.

RecoverSense tests a focused hypothesis: **can customer-specific payment timing and current customer context identify a better recovery window than a fixed schedule or a single population-level retry hour?**

> **Evaluation disclosure:** all benchmark numbers in this repository are from a controlled synthetic evaluation. They are not a claim of live production recovery performance.

## What is novel

1. **Personalized recovery windows** — timing is inferred from each customer's own successful payment history.
2. **Behaviour + context fusion** — historical timing is combined with a constrained contextual reasoner for customer intent.
3. **AI has no financial authority** — the LLM never computes money, overrides policy, or directly executes a payment.
4. **Deterministic policy gate** — retry limits, amount ceilings, expected-value thresholds, failure class and duplicate protections are enforced outside AI.
5. **Fresh state check** — the execution adapter checks current payment state immediately before scheduling a retry.
6. **Tamper-evident audit** — every decision is chained using SHA-256 hashes.
7. **Counterfactual evaluation** — every strategy is tested on the same held-out events with deterministic event/hour outcomes.
8. **Failure injection** — timeout, duplicate webhook, malformed AI output and policy rejection can be demonstrated live in DEMO_MODE.

## Architecture

```text
Payment Failure
      ↓
Webhook Verification
      ↓
Atomic Idempotency
      ↓
Failure Classification
      ↓
Customer History ────────── Customer Context
      ↓                            ↓
Timing Intelligence             LLM / fallback
      └──────────────┬─────────────┘
                     ↓
             Recovery Probability
                     ↓
             Expected Net Recovery
                     ↓
               Policy Gate
                     ↓
             Current State Check
                     ↓
              Execution Adapter
                /          \
          Razorpay       Simulator
                \          /
                  Outcome
                     ↓
             Audit Ledger
                     ↓
          Counterfactual Evaluation
```

## Verified benchmark (seed 42, 6,000 dev / 1,200 test)

| Strategy | Recovery rate | Net recovered |
|---|---:|---:|
| Fixed-Timer | 31.50% | ₹49,13,098.41 |
| Population Peak-Hour | 34.58% | ₹54,49,866.34 |
| **RecoverSense** | **35.00%** | **₹55,94,856.95** |

**Incremental vs Fixed-Timer:** ₹6,81,758.54  
**Incremental vs Population Peak-Hour:** ₹1,44,990.61

The multi-seed run is intentionally not cherry-picked. With 10 independent seeds at 3,000 dev / 800 test, the mean incremental net recovery was **₹4,18,251 vs Fixed-Timer** and **₹1,82,030 vs Population Peak-Hour**. The 95% interval for the latter crossed zero, so RecoverSense is **not** claimed to win every possible synthetic population.

## Run locally

### Backend

```bash
cd RecoverSense
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

# Demo mode — safe for local/buildathon use
set DEMO_MODE=true
set PRODUCTION_MODE=false

cd backend
uvicorn app.main:app --reload --port 8000
```

### Frontend

Serve `frontend/` with any static server. For example:

```bash
cd frontend
python -m http.server 5500
```

Open `http://localhost:5500`.

The frontend uses hash routing:

- `#/dashboard`
- `#/opportunities`
- `#/simulator`
- `#/evaluation`
- `#/audit`
- `#/failures`
- `#/settings`

## Production deployment

### 1. Required environment variables

The backend is configured entirely through environment variables — nothing in the
codebase contains a real key or secret. All values must be set by the platform
(secret manager, `--env-file`, or deployment UI). Never commit a `.env` file.

| Variable | Required | Purpose |
|---|---|---|
| `PRODUCTION_MODE` | yes | `true` for any real integration (including Razorpay Test Mode); enables API-key auth and fail-fast secret validation |
| `DEMO_MODE` | no (default `true`) | `false` in production; disables the failure-injection router |
| `RECOVERSENSE_API_KEY` | **yes in production** | Bearer value clients must send as `X-API-Key`; startup refuses to run without it |
| `RAZORPAY_WEBHOOK_SECRET` | **yes in production** | Webhook signing secret used to verify `X-Razorpay-Signature`; startup refuses to run without it |
| `DATABASE_URL` | no (default `sqlite:///./recoversense.db`) | SQLAlchemy URL. SQLite is safe for a **single-instance** deployment. For multi-instance/high availability use PostgreSQL (e.g. `postgresql://user:pass@host:5432/recoversense` — the `psycopg2-binary` driver is installed) |
| `CORS_ORIGINS` | no (default `http://localhost:5500,http://127.0.0.1:5500,http://localhost:5000,http://127.0.0.1:5000`) | Comma-separated allowed browser origins. **Never use `*`.** Unset/empty disables cross-origin access |
| `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` | no | Razorpay **Test Mode** credentials; used only for documented read-side state verification |
| `RAZORPAY_STATE_VERIFICATION_ENABLED` | no (default follows `PRODUCTION_MODE`) | Opt-in real provider state checks before a retry |
| `ENABLE_API_DOCS` | no (default: enabled outside production, **disabled when `PRODUCTION_MODE=true`**) | Controls `/docs`, `/redoc` and `/openapi.json`. Production deployments do not publicly expose the interactive API surface; set `true` to force docs on (not recommended) |
| `ANTHROPIC_API_KEY` | no | Enables the LLM context reasoner; omitted → transparent rule-based fallback |
| `RECOVERSENSE_MODEL_PATH` | no | Path to the trained model artifact (defaults to the bundled `artifacts/recovery_model.pkl`) |
| `MAX_BODY_BYTES` | no (default `1048576`) | Request body size cap |
| `LOG_LEVEL` | no (default `INFO`) | `DEBUG \| INFO \| WARNING \| ERROR`. Secret values are never logged at any level |

Generate the API key with `python -c "import secrets; print(secrets.token_urlsafe(48))"`.

### 2. Build

```bash
docker build -t recoversense .
```

The image runs as an unprivileged user, uses deterministic dependency pins, and
requires no build secrets. The trained recovery-model artifact
(`backend/artifacts/recovery_model.pkl`) is copied into the image by default and
is required for `PRODUCTION_MODE=true` — build after provisioning it, or mount a
replacement with `RECOVERSENSE_MODEL_PATH`.

The bundled artifact is trained with the **pinned scikit-learn version** and
regenerating it is one deterministic command (same seeded dev set and training
path as the demo bootstrap — run it whenever the sklearn pin changes, or a
stale pickle will fail to load):

```bash
python scripts/provision_production_model.py            # -> backend/artifacts/recovery_model.pkl
python scripts/provision_production_model.py /srv/models/recovery.pkl
```

### 3. Start

```bash
# Single instance (SQLite) — 1 worker only
docker run --rm -p 8000:8000 --env-file <your-env-file> recoversense

# PostgreSQL / multi-worker (each worker must reach the same DB)
docker run --rm -p 8000:8000 -e UVICORN_WORKERS=4 --env-file <your-env-file> recoversense
```

Port and worker count are runtime-configurable with `PORT` and `UVICORN_WORKERS`.
The container `HEALTHCHECK` polls `GET /health`. A plain `POST` from Razorpay
(`/webhooks/razorpay`) is the only external write path in production.

### 4. Health endpoint

- `GET /health` — liveness: `{"status": "healthy", ...}`. Never exposes secrets.
- `GET /readiness` — reports `database`, `configuration` and `model` checks.

### 5. Razorpay webhook endpoint

- `POST /webhooks/razorpay` — verifies the `X-Razorpay-Signature` HMAC-SHA256
  header against `RAZORPAY_WEBHOOK_SECRET` (fail-closed in production), ingests
  `payment.failed` / `subscription.charged.failed` events, and triggers the
  autonomous agent pipeline. Invalid signatures are rejected with `400` and
  logged as `webhook_rejected`.

### 6. Configure the Razorpay Test Mode webhook URL

1. Razorpay Dashboard → **Settings → Webhooks** (Test Mode).
2. Set the URL to `https://<your-deployed-host>/webhooks/razorpay`.
3. Subscribe to the failure events you want RecoverSense to see:
   `payment.failed` and (for mandates/subscriptions) `subscription.charged.failed`.
4. Save. Razorpay will now sign each delivery with the webhook secret below.

### 7. Configure the webhook secret

1. In the same Razorpay webhook settings, grab/copy the **webhook secret**.
2. Set it as `RAZORPAY_WEBHOOK_SECRET` in your deployment environment.
3. It must match exactly — RecoverSense recomputes the HMAC-SHA256 signature and
   rejects any delivery that does not match.

### 8. Which operations are genuinely supported

- **Webhook ingestion + signature verification** — real and verified.
- **Razorpay Test Mode read-side state checks** (`GET /v1/payments/:id` via
  `RazorpayTestModeAdapter`) — real, using `RAZORPAY_KEY_ID`/`RAZORPAY_KEY_SECRET`,
  when `RAZORPAY_STATE_VERIFICATION_ENABLED` is on.
- **Decisioning, policy gating, outcome recording, audit ledger** — fully local.

### 9. Which provider operations remain unsupported

- **Arbitrary scheduled UPI-mandate retry writes on the Razorpay API.** Razorpay's
  public API has no documented Test Mode endpoint to force a new future retry of a
  failed recurring payment server-side. RecoverSense therefore keeps the
  write-side "schedule retry" operation behind the clearly labelled
  `SimulatorExecutionAdapter` and **never pretends a simulated call is a real
  Razorpay response**. Deploying this image does **not** make that unsupported
  write real — a verified merchant retry path (webhook-triggered re-charge,
  `orders.create` + `payments.capture` for the specific confirmed flow, or
  Razorpay's own auto-retry) must be wired before real money moves.

### Database note (single-instance vs multi-instance)

The default SQLite database is acceptable for a **single-instance deployment**
(one process, one worker). It is not safe for multi-process or horizontally
scaled deployments: file locking serializes writers and an attached volume is
required. The code already supports PostgreSQL via `DATABASE_URL` (including a
`SELECT FOR UPDATE` ledger path), but this repository has **not** migrated any
databases — that remains a deployment/ops decision.

Production mode fails closed at startup when `RECOVERSENSE_API_KEY` or
`RAZORPAY_WEBHOOK_SECRET` is missing, or when the production model artifact
cannot be loaded. The failure-injection router is not mounted outside `DEMO_MODE`.

The Razorpay adapter deliberately does **not** invent an undocumented arbitrary
scheduled UPI retry API. It supports documented read-side state checks and keeps
the write-side scheduling operation behind a clearly labelled simulator adapter
until the merchant's exact supported retry mechanism is wired.

## Evaluation

```bash
python scripts/run_evaluation.py 6000 1200 42
python scripts/run_evaluation.py --multi-seed 3000 800
```

The simulator/evaluation outcome generator derives its local randomness from SHA-256 rather than Python's process-randomized `hash()`. Therefore the same event + hour is reproducible across processes.

## Evaluation & Observability

Once the backend is running, the following endpoints expose decision-level and portfolio-level observability derived from persisted records only:

```bash
# Structured decision trace for a specific event
curl http://localhost:8000/evaluation/decision-trace/{event_id}

# Compact evaluation summary (revenue at risk, recovered, strategy distribution, etc.)
curl http://localhost:8000/evaluation/summary

# Strategy-level comparison (only strategies with observed data)
curl http://localhost:8000/evaluation/strategy-comparison

# Audit chain integrity verification (read-only)
curl http://localhost:8000/evaluation/audit/verify
```

The decision trace makes the full economic reasoning explicit: model proposal → policy authorization → provider verification → execution status → observed outcome → internal ledger only → unsupported provider operation honesty. No unsupported provider operation is ever implied as having executed.

## Testing

```bash
python -m unittest discover -s backend/tests -v
```

The suite covers core decisioning, portfolio optimization, the audit chain, and
deployment/infrastructure guarantees (webhook signature fail-closed behavior,
production config validation, health endpoint secrecy, CORS parsing, template
hygiene). The release candidate has been verified with all tests passing.

## Important boundary

RecoverSense is a **recovery decisioning prototype / buildathon application**, not a certified payment-processing system. Before real-money deployment, complete merchant authentication/authorization, production secrets management, PostgreSQL deployment, model governance, observability, provider-specific payment retry integration, compliance/security review and operational controls.
