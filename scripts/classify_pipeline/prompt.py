"""Prompt template and response parser for the classification subagent."""

import json
from typing import Dict, Optional

from .schema import (
    CLASSIFICATION_FIELDS,
    CONFIDENCE_LEVELS,
    DISEASE_LABELS,
    MECHANISM_LABELS,
    RELEVANCE_LABELS,
    TOPIC_LABELS,
)

SYSTEM_PROMPT = (
    """\
You are a biomedical literature classification assistant.
Your task is to read a research article's metadata and assign structured labels.

## Output format

Return ONLY a single JSON object with these fields:
{
  "primary_topic": "<car_dc|car_mac|car_mono|car_t|car_nk>",
  "relevance": "<relevant|peripheral|irrelevant>",
  "primary_mechanism": "<mechanism label>",
  "secondary_mechanism": "<mechanism label or empty string>",
  "disease_label": "<disease label>",
  "disease_detail": "<specific disease name or empty string>",
  "confidence": "<high|medium|low>",
  "reason": "<1-2 sentence evidence-based justification>"
}

## Primary topic assignment

A paper may be matched by multiple topic queries. You must determine the ONE primary topic based on the paper's actual content:
- `car_dc`: the paper is primarily about CAR-engineered dendritic cells
- `car_mac`: the paper is primarily about CAR-engineered macrophages
- `car_mono`: the paper is primarily about CAR-engineered monocytes
- `car_t`: the paper is primarily about CAR T cells
- `car_nk`: the paper is primarily about CAR natural killer cells

Rules:
- Choose based on which CAR cell type the paper ACTUALLY studies, not which queries happened to match it.
- If the paper studies CAR-T cells but also mentions macrophages in the tumor microenvironment, primary_topic is `car_t`.
- If the paper is a general CAR platform/engineering study not specific to one cell type, choose the cell type most central to the experiments.
- If the paper primarily discusses both CAR-T and CAR-NK equally, prefer the cell type that is the main experimental subject.

## Relevance assessment

Judge whether this paper genuinely belongs to any CAR cell therapy topic:
- `relevant`: the paper is directly about CAR-engineered cells
- `peripheral`: the paper mentions CAR technology but is not primarily about it (e.g., a general immunotherapy review that briefly mentions CAR-T)
- `irrelevant`: the paper is a false positive — it matched the query due to keyword overlap but has nothing to do with CAR cell therapy

If relevance is `irrelevant`, still fill in the other fields with your best guess, but set confidence to `low`.

## Mechanism labels (pick exactly one for primary_mechanism)

"""
    + "\n".join(f"- `{m}`" for m in MECHANISM_LABELS)
    + """

Rules:
- `cytotoxic_killing`: direct tumor cell killing via CAR-mediated cytotoxicity (perforin/granzyme, ADCC, etc.)
- `phagocytosis`: CAR-mediated engulfment of target cells (mainly CAR-Mac)
- `immune_regulation`: modulating immune responses, Treg induction, cytokine modulation, checkpoint interaction
- `microenvironment_remodeling`: reshaping the tumor or tissue microenvironment, ECM degradation, TAM repolarization
- `antigen_presentation`: enhancing antigen presentation to activate adaptive immunity (mainly CAR-DC)
- `fibrosis_modulation`: targeting fibrotic tissue or fibrosis-related pathways
- `drug_delivery_or_platform`: using CAR cells as delivery vehicles, or describing platform/vector design without specific mechanism
- `manufacturing_or_engineering`: CAR construct design, cell manufacturing, gene editing, production optimization
- `safety_or_toxicity`: CRS management, neurotoxicity, on-target off-tumor effects, safety monitoring
- `diagnostic_or_monitoring`: biomarkers, imaging, MRD detection, response monitoring
- `other`: none of the above categories clearly applies

Only assign `secondary_mechanism` if the paper genuinely describes TWO mechanisms with comparable importance. Leave it as empty string if unsure.

## Disease labels (pick exactly one for disease_label)

"""
    + "\n".join(f"- `{d}`" for d in DISEASE_LABELS)
    + """

Rules:
- `cancer`: any malignancy (solid tumor or hematologic)
- `autoimmune_disease`: lupus, rheumatoid arthritis, MS, type 1 diabetes, etc.
- `organ_fibrosis`: pulmonary fibrosis, liver fibrosis, cardiac fibrosis, etc.
- `infectious_disease`: HIV, HBV, CMV, fungal infections, etc.
- `hematologic_disorder_nonmalignant`: sickle cell disease, thalassemia, etc. (NOT leukemia/lymphoma)
- `transplantation`: GvHD, transplant rejection, tolerance induction
- `inflammatory_disease`: IBD, Crohn's, non-autoimmune inflammatory conditions
- `neurologic_disease`: neurodegeneration, glioma counts as cancer not here
- `other`: platform/engineering study with no specific disease context, or disease not fitting above

For `disease_detail`, write the specific disease when identifiable (e.g., "B-cell acute lymphoblastic leukemia", "systemic lupus erythematosus", "idiopathic pulmonary fibrosis"). Leave empty if the paper is about a general platform or the disease is unclear.

## Confidence guidelines

- `high`: clear disease context AND clear mechanism from abstract
- `medium`: partial information, some inference needed
- `low`: abstract missing/very short, or highly ambiguous content
"""
)


def build_user_message(record: Dict[str, str]) -> str:
    """Build the user-side prompt for a single article."""
    parts = [
        f"PMID: {record.get('pmid', '')}",
        f"Title: {record.get('title', '')}",
    ]
    abstract = record.get("abstract", "").strip()
    if abstract:
        if len(abstract) > 4000:
            abstract = abstract[:4000] + "... [truncated]"
        parts.append(f"Abstract: {abstract}")
    else:
        parts.append("Abstract: (not available)")

    mesh = record.get("mesh_terms", "").strip()
    if mesh:
        parts.append(f"MeSH Terms: {mesh}")

    topics = record.get("matched_topics", "").strip()
    if topics:
        parts.append(f"Matched Topics: {topics}")

    return "\n\n".join(parts)


def parse_llm_response(raw_text: str) -> Optional[Dict[str, str]]:
    """Extract the JSON classification object from the LLM response.

    Handles responses that may contain markdown code fences or
    extra text around the JSON.
    """
    text = raw_text.strip()

    # Strip markdown code fences if present
    if "```" in text:
        parts = text.split("```")
        for part in parts[1:]:
            candidate = part.strip()
            if candidate.lower().startswith("json"):
                candidate = candidate[4:].strip()
            if candidate.startswith("{"):
                text = candidate
                break

    # Find the JSON object boundaries
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    json_str = text[start : end + 1]
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        return None

    if not isinstance(parsed, dict):
        return None

    # Normalize field values
    result: Dict[str, str] = {}
    for field in CLASSIFICATION_FIELDS:
        value = str(parsed.get(field, "")).strip()
        result[field] = value

    return result
