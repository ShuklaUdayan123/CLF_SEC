"""
impact_model.py — Prioritization Matrix
=========================================
Ingests the enriched dataset from the ingestion layer and produces
a ranked, categorized action list using a two-axis scoring model:

    • **Ease of Implementation** (1–10)  — inverse of keyword difficulty
      and position gap (easier to close → higher score).
    • **Potential Reach** (1–10) — composite of search volume and
      conversion rate (higher business value → higher score).

Each query is categorized as a "Quick Win" or "Major Project" based
on configurable thresholds.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Default thresholds                                                 #
# ------------------------------------------------------------------ #

DEFAULT_QUICK_WIN_EASE_MIN: int = 7
DEFAULT_QUICK_WIN_REACH_MIN: int = 5


# ------------------------------------------------------------------ #
#  Public API                                                         #
# ------------------------------------------------------------------ #

def calculate_priority_score(
    df: pd.DataFrame,
    ease_weight_difficulty: float = 0.6,
    ease_weight_gap: float = 0.4,
    reach_weight_volume: float = 0.3,
    reach_weight_visitors: float = 0.3,
    reach_weight_conversion: float = 0.3,
    reach_weight_bounce: float = 0.1,
    quick_win_ease_min: int = DEFAULT_QUICK_WIN_EASE_MIN,
    quick_win_reach_min: int = DEFAULT_QUICK_WIN_REACH_MIN,
) -> pd.DataFrame:
    """Score and rank every query in *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain at minimum: ``query``, ``keyword_difficulty``,
        ``position_gap``, ``search_volume``, ``conversion_rate_pct``.
    ease_weight_difficulty : float
        Weight of keyword-difficulty in the Ease score (0–1).
    ease_weight_gap : float
        Weight of position-gap in the Ease score (0–1).
    reach_weight_volume : float
        Weight of search-volume in the Reach score (0–1).
    reach_weight_conversion : float
        Weight of conversion-rate in the Reach score (0–1).
    quick_win_ease_min : int
        Minimum Ease score to qualify as a Quick Win.
    quick_win_reach_min : int
        Minimum Reach score to qualify as a Quick Win.

    Returns
    -------
    pd.DataFrame
        Original columns plus ``ease_score``, ``reach_score``,
        ``composite_score``, and ``category``.  Sorted descending
        by ``composite_score``.
    """
    required_cols = {"query", "keyword_difficulty", "position_gap",
                     "search_volume", "conversion_rate_pct",
                     "unique_visitors", "bounce_rate_pct"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input DataFrame is missing columns: {missing}")

    result = df.copy()

    # -- Ease of Implementation (1–10) --------------------------------
    # Lower keyword difficulty → easier → higher score
    result["_norm_difficulty"] = _normalize_inverted(result["keyword_difficulty"])
    # Smaller position gap → easier to close → higher score
    result["_norm_gap"] = _normalize_inverted(result["position_gap"])

    result["ease_score"] = (
        ease_weight_difficulty * result["_norm_difficulty"]
        + ease_weight_gap * result["_norm_gap"]
    ).round(1)

    # -- Potential Reach (1–10) ----------------------------------------
    result["_norm_volume"] = _normalize(result["search_volume"])
    result["_norm_conversion"] = _normalize(result["conversion_rate_pct"])
    result["_norm_visitors"] = _normalize(result["unique_visitors"])
    result["_norm_bounce"] = _normalize_inverted(result["bounce_rate_pct"])

    result["reach_score"] = (
        reach_weight_volume * result["_norm_volume"]
        + reach_weight_visitors * result["_norm_visitors"]
        + reach_weight_conversion * result["_norm_conversion"]
        + reach_weight_bounce * result["_norm_bounce"]
    ).round(1)

    # -- Composite Score -----------------------------------------------
    result["composite_score"] = (
        (result["ease_score"] + result["reach_score"]) / 2
    ).round(2)

    # -- Categorization ------------------------------------------------
    result["category"] = np.where(
        (result["ease_score"] >= quick_win_ease_min)
        & (result["reach_score"] >= quick_win_reach_min),
        "Quick Win",
        "Major Project",
    )

    # Clean up helper columns
    result.drop(
        columns=["_norm_difficulty", "_norm_gap", "_norm_volume", 
                 "_norm_conversion", "_norm_visitors", "_norm_bounce"],
        inplace=True,
    )

    result.sort_values("composite_score", ascending=False, inplace=True)
    result.reset_index(drop=True, inplace=True)

    quick_wins = (result["category"] == "Quick Win").sum()
    major_projects = (result["category"] == "Major Project").sum()
    logger.info(
        "Prioritization complete: %d Quick Wins, %d Major Projects",
        quick_wins,
        major_projects,
    )

    return result


# ------------------------------------------------------------------ #
#  Internal helpers                                                   #
# ------------------------------------------------------------------ #

def _normalize(series: pd.Series, low: float = 1.0, high: float = 10.0) -> pd.Series:
    """Min-max normalize *series* into [low, high]."""
    s_min, s_max = series.min(), series.max()
    if s_max == s_min:
        return pd.Series([(low + high) / 2] * len(series), index=series.index)
    return low + (series - s_min) / (s_max - s_min) * (high - low)


def _normalize_inverted(series: pd.Series, low: float = 1.0, high: float = 10.0) -> pd.Series:
    """Min-max normalize *series* into [low, high] with inversion
    (highest raw value → lowest normalized score)."""
    s_min, s_max = series.min(), series.max()
    if s_max == s_min:
        return pd.Series([(low + high) / 2] * len(series), index=series.index)
    return high - (series - s_min) / (s_max - s_min) * (high - low)


# ------------------------------------------------------------------ #
#  CLI convenience                                                    #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Add the workspace root to sys.path so the absolute import works when run directly
    workspace_root = Path(__file__).resolve().parents[3] 
    if str(workspace_root) not in sys.path:
        sys.path.insert(0, str(workspace_root))

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    # Pull enriched data from the ingestion layer
    from cloudflare_ai_pipeline.src.ingestion.data_extractor import build_enriched_dataset

    enriched = build_enriched_dataset()
    ranked = calculate_priority_score(enriched)

    print("\n🏆  Prioritized Action List:\n")
    display_cols = [
        "query", "ease_score", "reach_score",
        "composite_score", "category"
    ]
    if "ai_sentiment" in ranked.columns:
        display_cols.append("ai_sentiment")
        
    print(ranked[display_cols].to_string(index=False))
