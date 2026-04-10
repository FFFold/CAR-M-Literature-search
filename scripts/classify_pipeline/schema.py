"""Classification label definitions for mechanism and disease tagging.

These sets define the valid label values that the LLM subagent and the
rules validator accept.  Adding a new label here automatically makes it
valid throughout the pipeline.
"""

MECHANISM_LABELS = (
    "cytotoxic_killing",
    "phagocytosis",
    "immune_regulation",
    "microenvironment_remodeling",
    "antigen_presentation",
    "fibrosis_modulation",
    "drug_delivery_or_platform",
    "manufacturing_or_engineering",
    "safety_or_toxicity",
    "diagnostic_or_monitoring",
    "other",
)

DISEASE_LABELS = (
    "cancer",
    "autoimmune_disease",
    "organ_fibrosis",
    "infectious_disease",
    "hematologic_disorder_nonmalignant",
    "transplantation",
    "inflammatory_disease",
    "neurologic_disease",
    "other",
)

RELEVANCE_LABELS = ("relevant", "peripheral", "irrelevant")

CONFIDENCE_LEVELS = ("high", "medium", "low")

CLASSIFICATION_FIELDS = (
    "relevance",
    "primary_mechanism",
    "secondary_mechanism",
    "disease_label",
    "disease_detail",
    "confidence",
    "reason",
)
