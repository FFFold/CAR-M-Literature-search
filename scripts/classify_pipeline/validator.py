"""Rules-based validation layer for classification outputs.

Checks LLM outputs for schema conformance and flags records that
need manual review.
"""

from typing import Dict, List, Tuple

from .schema import CONFIDENCE_LEVELS, DISEASE_LABELS, MECHANISM_LABELS


def validate_classification(
    record: Dict[str, str], classification: Dict[str, str]
) -> Tuple[Dict[str, str], List[str]]:
    """Validate and normalize a classification result.

    Returns a tuple of (normalized_classification, review_reasons).
    review_reasons is empty if the classification passes all checks.
    """
    result = dict(classification)
    review_reasons: List[str] = []

    # --- Validate primary_mechanism ---
    pm = result.get("primary_mechanism", "").strip().lower()
    if pm not in MECHANISM_LABELS:
        review_reasons.append(f"invalid_primary_mechanism:{pm}")
        pm = "other"
    result["primary_mechanism"] = pm

    # --- Validate secondary_mechanism ---
    sm = result.get("secondary_mechanism", "").strip().lower()
    if sm and sm not in MECHANISM_LABELS:
        review_reasons.append(f"invalid_secondary_mechanism:{sm}")
        sm = ""
    if sm == pm:
        sm = ""  # Don't duplicate primary
    result["secondary_mechanism"] = sm

    # --- Validate disease_label ---
    dl = result.get("disease_label", "").strip().lower()
    if dl not in DISEASE_LABELS:
        review_reasons.append(f"invalid_disease_label:{dl}")
        dl = "other"
    result["disease_label"] = dl

    # --- Validate confidence ---
    conf = result.get("confidence", "").strip().lower()
    if conf not in CONFIDENCE_LEVELS:
        review_reasons.append(f"invalid_confidence:{conf}")
        conf = "low"
    result["confidence"] = conf

    # --- Check for low confidence ---
    if conf == "low":
        review_reasons.append("low_confidence")

    # --- Check abstract quality ---
    abstract = record.get("abstract", "").strip()
    if not abstract:
        if conf != "low":
            result["confidence"] = "low"
        review_reasons.append("missing_abstract")
    elif len(abstract) < 200:
        review_reasons.append("short_abstract")

    # --- Normalize disease_detail ---
    result["disease_detail"] = result.get("disease_detail", "").strip()

    # --- Normalize reason ---
    result["reason"] = result.get("reason", "").strip()

    return result, review_reasons


def make_fallback_classification(
    record: Dict[str, str],
) -> Tuple[Dict[str, str], List[str]]:
    """Create a minimal fallback classification when the LLM fails.

    Used when the LLM returns nothing parseable after all retries.
    """
    result = {
        "primary_mechanism": "other",
        "secondary_mechanism": "",
        "disease_label": "other",
        "disease_detail": "",
        "confidence": "low",
        "reason": "LLM classification failed; fallback assignment.",
    }
    review_reasons = ["llm_failure", "low_confidence"]

    abstract = record.get("abstract", "").strip()
    if not abstract:
        review_reasons.append("missing_abstract")

    return result, review_reasons
