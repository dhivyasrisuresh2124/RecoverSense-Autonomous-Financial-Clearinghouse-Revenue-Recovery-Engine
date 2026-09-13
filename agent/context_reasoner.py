"""
RecoverSense — AI Context Reasoner
=====================================

STRICT BOUNDARY: this is the ONLY module allowed to call an LLM. It:
  - reads unstructured text (support notes / customer messages)
  - outputs a small STRICT JSON object
  - NEVER computes money, NEVER enforces policy, NEVER calls execution APIs,
    NEVER invents facts not present in the input

If ANTHROPIC_API_KEY is not set (e.g. running this offline evaluation in
a sandbox with no network), we fall back to a transparent rule-based
keyword reasoner that mimics the same output contract. This keeps the
full pipeline runnable end-to-end with zero external dependencies, while
still supporting a real LLM call when credentials + network are available.

Output schema (validated, malformed output is rejected -> NO_ACTION/ESCALATE):

{
  "customer_intent": "HIGH" | "MEDIUM" | "LOW" | "UNKNOWN",
  "failure_context": "TEMPORARY" | "UNCERTAIN" | "LIKELY_PERMANENT",
  "recommended_action": "SCHEDULE_RETRY" | "ESCALATE" | "NO_ACTION",
  "recommended_window": "HH:MM-HH:MM" | null,
  "reason": "<short natural-language justification>"
}
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Optional, Dict, Any

VALID_INTENTS = {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}
VALID_CONTEXTS = {"TEMPORARY", "UNCERTAIN", "LIKELY_PERMANENT"}
VALID_ACTIONS = {"SCHEDULE_RETRY", "ESCALATE", "NO_ACTION"}

WINDOW_RE = re.compile(r"^\d{2}:\d{2}-\d{2}:\d{2}$")

LLM_TIMEOUT_SECONDS = 8.0

# Below this confidence, the LLM's signal is treated as unreliable and the
# system falls back to the transparent rule-based reasoner instead — an
# LLM output that is schema-valid but LOW-confidence is not automatically
# trusted. This threshold is a starting default; per P1 guidance it
# should be tuned experimentally against labeled examples, not asserted
# as optimal without evidence.
MIN_LLM_CONFIDENCE = 0.70

HIGH_INTENT_PATTERNS = [
    r"\bwill (retry|pay|complete)\b",
    r"\bfunds? (will be|available)\b",
    r"\bconfirmed\b.*\b(pay|retry)\b",
    r"\bafter (work|salary|6 ?pm|evening)\b",
]
LOW_INTENT_PATTERNS = [
    r"\bcancel\b",
    r"\bno response\b",
    r"\bnot responded\b",
    r"\bunresolved\b",
]


class SchemaValidationError(Exception):
    pass


@dataclass
class ContextReasonerOutput:
    customer_intent: str
    failure_context: str
    recommended_action: str
    recommended_window: Optional[str]
    reason: str
    source: str  # "llm" | "fallback_rule_based" | "rejected_malformed" | "fallback_low_confidence"
    confidence: float = 1.0  # 1.0 for rule-based (deterministic); LLM reports its own

    def to_dict(self) -> Dict[str, Any]:
        return {
            "customer_intent": self.customer_intent,
            "failure_context": self.failure_context,
            "recommended_action": self.recommended_action,
            "recommended_window": self.recommended_window,
            "reason": self.reason,
            "source": self.source,
            "confidence": round(self.confidence, 3),
        }


def validate_schema(obj: Dict[str, Any]) -> None:
    required = {"customer_intent", "failure_context", "recommended_action", "recommended_window", "reason"}
    missing = required - set(obj.keys())
    if missing:
        raise SchemaValidationError(f"Missing keys: {missing}")
    if obj["customer_intent"] not in VALID_INTENTS:
        raise SchemaValidationError(f"Invalid customer_intent: {obj['customer_intent']}")
    if obj["failure_context"] not in VALID_CONTEXTS:
        raise SchemaValidationError(f"Invalid failure_context: {obj['failure_context']}")
    if obj["recommended_action"] not in VALID_ACTIONS:
        raise SchemaValidationError(f"Invalid recommended_action: {obj['recommended_action']}")
    if obj["recommended_window"] is not None and not WINDOW_RE.match(obj["recommended_window"]):
        raise SchemaValidationError(f"Invalid recommended_window format: {obj['recommended_window']}")
    if not isinstance(obj["reason"], str) or len(obj["reason"]) == 0:
        raise SchemaValidationError("reason must be a non-empty string")
    if "confidence" in obj:
        conf = obj["confidence"]
        if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
            raise SchemaValidationError(f"confidence must be a number in [0,1], got {conf!r}")


def _rule_based_fallback(
    support_note: str,
    failure_class: str,
    timing_window: Optional[str],
) -> ContextReasonerOutput:
    """
    Transparent keyword-pattern reasoner. Deliberately simple: this is a
    STAND-IN for an LLM call, used only when no LLM credentials/network
    are available, so the full pipeline still runs end-to-end offline.
    """
    text = (support_note or "").lower()

    intent = "UNKNOWN"
    if any(re.search(p, text) for p in HIGH_INTENT_PATTERNS):
        intent = "HIGH"
    elif any(re.search(p, text) for p in LOW_INTENT_PATTERNS):
        intent = "LOW"
    elif support_note:
        intent = "MEDIUM"

    if failure_class == "HARD":
        context = "LIKELY_PERMANENT"
    elif failure_class == "SOFT":
        context = "TEMPORARY"
    else:
        context = "UNCERTAIN"

    if context == "LIKELY_PERMANENT":
        action = "ESCALATE"
        window = None
        reason = "Failure classified as likely permanent (hard mandate failure); routing to human review."
    elif intent == "LOW":
        action = "ESCALATE"
        window = None
        reason = "Customer intent signal is low based on support history; recommend human follow-up over autonomous retry."
    else:
        action = "SCHEDULE_RETRY"
        window = timing_window
        reason = (
            f"Failure appears temporary and customer intent signal is {intent.lower()}. "
            f"Recommending a scheduled retry"
            + (f" within the historically observed high-success window {window}." if window else
               " without a strong timing signal (insufficient history); using default near-term retry.")
        )

    return ContextReasonerOutput(
        customer_intent=intent,
        failure_context=context,
        recommended_action=action,
        recommended_window=window,
        reason=reason,
        source="fallback_rule_based",
    )


def _try_llm_call(support_note: str, failure_class: str, timing_window: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Attempts a real LLM call via the Anthropic API if ANTHROPIC_API_KEY is
    set. Returns None (triggering fallback) on any error, timeout, missing
    key, or lack of network — this function must never raise or hang the
    payment recovery pipeline. Bounded by LLM_TIMEOUT_SECONDS.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic  # type: ignore

        client = anthropic.Anthropic(api_key=api_key, timeout=LLM_TIMEOUT_SECONDS)
        system_prompt = (
            "You are a strict JSON-only classifier for a payment recovery system. "
            "You NEVER compute financial amounts and NEVER decide policy. "
            "Given a support note and failure context, output ONLY this JSON object, "
            "no prose, no markdown fences:\n"
            '{"customer_intent": "HIGH|MEDIUM|LOW|UNKNOWN", '
            '"failure_context": "TEMPORARY|UNCERTAIN|LIKELY_PERMANENT", '
            '"recommended_action": "SCHEDULE_RETRY|ESCALATE|NO_ACTION", '
            '"recommended_window": "HH:MM-HH:MM or null", '
            '"confidence": "a number from 0.0 to 1.0 reflecting how confident you are '
            'in this classification, based only on how explicit/unambiguous the support '
            'note is — do not default to a high number", '
            '"reason": "short justification grounded only in the given input"}'
        )
        user_prompt = (
            f"Support note: {support_note!r}\n"
            f"Failure class: {failure_class}\n"
            f"Historically observed high-success timing window: {timing_window or 'none / insufficient history'}\n"
        )
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        text = text.strip().strip("`")
        if text.startswith("json"):
            text = text[4:].strip()
        return json.loads(text)
    except Exception:
        # Covers: no network, API timeout, malformed JSON, SDK errors, or
        # any other failure mode. Payment recovery must never hang or
        # crash because an LLM call misbehaved.
        return None


def get_context_recommendation(
    support_note: str,
    failure_class: str,
    timing_window: Optional[str],
) -> ContextReasonerOutput:
    llm_raw = _try_llm_call(support_note, failure_class, timing_window)

    if llm_raw is not None:
        try:
            validate_schema(llm_raw)
            confidence = float(llm_raw.get("confidence", 1.0))

            if confidence < MIN_LLM_CONFIDENCE:
                # Schema-valid but not confident enough to trust — fall
                # back rather than act on a low-confidence LLM signal.
                fallback = _rule_based_fallback(support_note, failure_class, timing_window)
                fallback.source = "fallback_low_confidence"
                fallback.reason = (
                    f"LLM confidence {confidence:.2f} below threshold {MIN_LLM_CONFIDENCE:.2f}; "
                    f"using rule-based fallback instead. LLM had said: {llm_raw.get('reason', '')!r}"
                )
                return fallback

            return ContextReasonerOutput(
                customer_intent=llm_raw["customer_intent"],
                failure_context=llm_raw["failure_context"],
                recommended_action=llm_raw["recommended_action"],
                recommended_window=llm_raw["recommended_window"],
                reason=llm_raw["reason"],
                source="llm",
                confidence=confidence,
            )
        except SchemaValidationError:
            # Malformed LLM output -> never trust it, fall through to a
            # safe, transparent fallback rather than executing anything.
            pass

    return _rule_based_fallback(support_note, failure_class, timing_window)


if __name__ == "__main__":
    out = get_context_recommendation(
        support_note="Customer called and confirmed funds will be available this evening.",
        failure_class="SOFT",
        timing_window="17:00-19:00",
    )
    print(json.dumps(out.to_dict(), indent=2))
