"""
executive_summary.py — Reporting Layer
========================================
Consumes the prioritized DataFrame and an optional citation
DataFrame to produce a polished Markdown executive report.

The report includes:
  • Baseline visibility-gap overview
  • Top 3 Quick Wins for AEO / CAO optimization
  • Full ranked action table
  • PR "hit list" of cited community URLs
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Default output path                                                #
# ------------------------------------------------------------------ #

_DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"


# ------------------------------------------------------------------ #
#  Public API                                                         #
# ------------------------------------------------------------------ #

def generate_report(
    ranked_df: pd.DataFrame,
    citations_df: Optional[pd.DataFrame] = None,
    output_dir: Optional[str | Path] = None,
    filename: str = "executive_report.md",
) -> Path:
    """Generate a Markdown executive report and write it to disk.

    Parameters
    ----------
    ranked_df : pd.DataFrame
        Output of ``impact_model.calculate_priority_score()``.
        Required columns: query, ease_score, reach_score,
        composite_score, category, position_gap, search_volume,
        conversion_rate_pct.
    citations_df : pd.DataFrame, optional
        Output of ``nlp_analyzer.extract_citations()``.
        If provided, appended as a PR "hit list" section.
    output_dir : str or Path, optional
        Directory to write the report into.  Created if it does
        not exist.  Defaults to ``<project_root>/output``.
    filename : str
        Name of the Markdown file.

    Returns
    -------
    Path
        Absolute path to the generated report file.
    """
    out = Path(output_dir) if output_dir else _DEFAULT_OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / filename

    sections: list[str] = [
        _header(),
        _visibility_gap_summary(ranked_df),
        _quick_wins_section(ranked_df),
        _full_ranked_table(ranked_df),
    ]

    if citations_df is not None and not citations_df.empty:
        sections.append(_pr_hit_list(citations_df))

    sections.append(_footer())

    report_content = "\n\n".join(sections)
    report_path.write_text(report_content, encoding="utf-8")

    logger.info("Executive report written to %s", report_path)
    return report_path


# ------------------------------------------------------------------ #
#  Section builders                                                   #
# ------------------------------------------------------------------ #

def _header() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        "# 📊 Cloudflare AI Visibility — Executive Report\n\n"
        f"> **Generated:** {ts}  \n"
        "> **Pipeline:** cloudflare_ai_pipeline v1.0  \n"
        "> **Methodology:** SEMrush Keyword Gap × Adobe Analytics Business Value  \n"
    )


def _visibility_gap_summary(df: pd.DataFrame) -> str:
    total_queries = len(df)
    avg_gap = df["position_gap"].mean()
    max_gap = df["position_gap"].max()
    worst_query = df.loc[df["position_gap"].idxmax(), "query"]
    total_volume = df["search_volume"].sum()
    total_visitors = df["unique_visitors"].sum()
    avg_conversion = df["conversion_rate_pct"].mean()
    avg_bounce = df["bounce_rate_pct"].mean()

    return (
        "## 1 — Baseline Visibility Gaps\n\n"
        "| Metric | Value |\n"
        "|--------|-------|\n"
        f"| Queries analyzed | **{total_queries}** |\n"
        f"| Avg. position gap vs AWS Lambda | **{avg_gap:.1f}** |\n"
        f"| Worst gap | **{max_gap}** (`{worst_query}`) |\n"
        f"| Combined monthly search volume | **{total_volume:,}** |\n"
        f"| Current unique visitors | **{total_visitors:,}** |\n"
        f"| Avg. conversion rate | **{avg_conversion:.1f}%** |\n"
        f"| Avg. bounce rate | **{avg_bounce:.1f}%** |\n\n"
        "Cloudflare trails AWS Lambda across the above queries.  "
        "Closing even a fraction of these gaps represents significant "
        "organic-traffic and pipeline opportunity."
    )


def _quick_wins_section(df: pd.DataFrame) -> str:
    quick_wins = df[df["category"] == "Quick Win"].head(3)

    if quick_wins.empty:
        return (
            "## 2 — Top Quick Wins for AEO / CAO Optimization\n\n"
            "> ⚠️ No queries met the Quick Win thresholds.  Consider "
            "lowering the ease or reach minimums in `config/settings.yaml`."
        )

    lines = [
        "## 2 — Top Quick Wins for AEO / CAO Optimization\n",
        "These queries offer the best ratio of implementation ease to "
        "potential business impact.  Prioritize these for immediate "
        "Answer Engine Optimization (AEO) and Conversational AI "
        "Optimization (CAO) efforts.\n",
    ]

    for rank, (_, row) in enumerate(quick_wins.iterrows(), start=1):
        lines.append(
            f"### #{rank}: `{row['query']}`\n"
            f"- **Composite Score:** {row['composite_score']}  \n"
            f"- **AI Sentiment:** **{row.get('ai_sentiment', 'Neutral')}**  \n"
            f"- **Ease:** {row['ease_score']} / 10 — **Reach:** {row['reach_score']} / 10  \n"
            f"- **Position Gap:** {row['position_gap']} (CF #{row.get('cf_position', '?')} vs AWS #{row.get('aws_position', '?')})  \n"
            f"- **Monthly Volume:** {row['search_volume']:,} — **Current Visitors:** {row['unique_visitors']:,}  \n"
            f"- **Conversion Rate:** {row['conversion_rate_pct']:.1f}% — **Bounce Rate:** {row['bounce_rate_pct']:.1f}%  \n"
            f"- **Recommended Action:** Produce targeted developer-docs content, "
            f"optimize structured data, and submit to AI training corpora.\n"
        )

    return "\n".join(lines)


def _full_ranked_table(df: pd.DataFrame) -> str:
    display_cols = [
        "query", "ai_sentiment", "ease_score", "reach_score",
        "composite_score", "category", "position_gap", 
        "search_volume", "unique_visitors", "bounce_rate_pct"
    ]
    available = [c for c in display_cols if c in df.columns]
    table_df = df[available].copy()

    header = "| " + " | ".join(available) + " |"
    sep = "| " + " | ".join(["---"] * len(available)) + " |"

    rows: list[str] = []
    for _, row in table_df.iterrows():
        cells = []
        for col in available:
            val = row[col]
            if isinstance(val, float):
                cells.append(f"{val:.1f}" if val != int(val) else str(int(val)))
            elif isinstance(val, (int, np.integer)):
                cells.append(f"{val:,}")
            else:
                cells.append(str(val))
        rows.append("| " + " | ".join(cells) + " |")

    return (
        "## 3 — Full Prioritized Action List\n\n"
        + header + "\n" + sep + "\n" + "\n".join(rows)
    )


def _pr_hit_list(citations_df: pd.DataFrame) -> str:
    lines = [
        "## 4 — PR & Community \"Hit List\"\n",
        "The following URLs were cited in AI-generated answers about "
        "Cloudflare.  Engaging with these sources (correcting "
        "misinformation, contributing authoritative content, or "
        "requesting updates) can directly improve AI model training "
        "data and future answer quality.\n",
        "| Priority | Source | URL |",
        "|----------|--------|-----|",
    ]

    for _, row in citations_df.iterrows():
        lines.append(
            f"| {row['priority']} | {row['source_label']} | {row['url']} |"
        )

    return "\n".join(lines)


def _footer() -> str:
    return (
        "---\n\n"
        "*This report was auto-generated by the Cloudflare AI Visibility Pipeline.  "
        "For methodology details, see the project README.*"
    )


# ------------------------------------------------------------------ #
#  Avoid bare numpy import at module level for the table builder       #
# ------------------------------------------------------------------ #
import numpy as np  # noqa: E402  (used in _full_ranked_table)


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

    # Run the full pipeline end-to-end
    from cloudflare_ai_pipeline.src.ingestion.data_extractor import build_enriched_dataset
    from cloudflare_ai_pipeline.src.processing.impact_model import calculate_priority_score
    from cloudflare_ai_pipeline.src.processing.nlp_analyzer import extract_citations

    enriched = build_enriched_dataset()
    ranked = calculate_priority_score(enriched)

    sample_text = (
        "See https://www.reddit.com/r/cloudflare/comments/abc for discussion. "
        "Also https://stackoverflow.com/questions/999/edge-workers and "
        "https://github.com/cloudflare/workers-sdk/issues/42."
    )
    citations = extract_citations(sample_text)

    path = generate_report(ranked, citations)
    print(f"\n✅  Report generated: {path}")
