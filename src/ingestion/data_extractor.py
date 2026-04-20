"""
data_extractor.py — Ingestion Layer
====================================
Responsible for pulling raw competitive-intelligence data from
SEMrush (keyword gap) and Adobe Analytics (traffic metrics),
then merging them into a single enriched DataFrame.

In production, swap the simulated helpers with real HTTP calls
using the credentials in config/settings.yaml.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Constants — simulated data mirrors the shape of real API responses #
# ------------------------------------------------------------------ #

_SIMULATED_SEMRUSH_DATA: list[dict] = [
    {"query": "serverless edge computing",        "cf_position": 14, "aws_position": 3,  "search_volume": 12_400, "keyword_difficulty": 72},
    {"query": "workers AI inference",             "cf_position": 18, "aws_position": 5,  "search_volume": 8_200,  "keyword_difficulty": 65},
    {"query": "deploy machine learning at edge",  "cf_position": 22, "aws_position": 2,  "search_volume": 6_800,  "keyword_difficulty": 78},
    {"query": "AI gateway API management",        "cf_position": 9,  "aws_position": 4,  "search_volume": 5_100,  "keyword_difficulty": 58},
    {"query": "edge function cold start",         "cf_position": 25, "aws_position": 6,  "search_volume": 4_300,  "keyword_difficulty": 44},
    {"query": "vector database serverless",       "cf_position": 30, "aws_position": 8,  "search_volume": 9_600,  "keyword_difficulty": 69},
    {"query": "LLM inference latency benchmark",  "cf_position": 20, "aws_position": 7,  "search_volume": 3_700,  "keyword_difficulty": 55},
    {"query": "serverless GPU compute",           "cf_position": 35, "aws_position": 1,  "search_volume": 11_000, "keyword_difficulty": 81},
    {"query": "cloudflare workers vs lambda",     "cf_position": 5,  "aws_position": 3,  "search_volume": 7_500,  "keyword_difficulty": 40},
    {"query": "edge AI model serving",            "cf_position": 28, "aws_position": 4,  "search_volume": 5_900,  "keyword_difficulty": 63},
]

_SIMULATED_ADOBE_DATA: list[dict] = [
    {"query": "serverless edge computing",        "avg_time_on_page_sec": 134, "conversion_rate_pct": 2.1, "unique_visitors": 4500, "bounce_rate_pct": 45.2},
    {"query": "workers AI inference",             "avg_time_on_page_sec": 98,  "conversion_rate_pct": 3.4, "unique_visitors": 1200, "bounce_rate_pct": 38.5},
    {"query": "deploy machine learning at edge",  "avg_time_on_page_sec": 112, "conversion_rate_pct": 1.8, "unique_visitors": 800,  "bounce_rate_pct": 62.1},
    {"query": "AI gateway API management",        "avg_time_on_page_sec": 145, "conversion_rate_pct": 4.2, "unique_visitors": 2100, "bounce_rate_pct": 25.4},
    {"query": "edge function cold start",         "avg_time_on_page_sec": 67,  "conversion_rate_pct": 1.5, "unique_visitors": 300,  "bounce_rate_pct": 81.0},
    {"query": "vector database serverless",       "avg_time_on_page_sec": 89,  "conversion_rate_pct": 2.8, "unique_visitors": 1500, "bounce_rate_pct": 55.6},
    {"query": "LLM inference latency benchmark",  "avg_time_on_page_sec": 76,  "conversion_rate_pct": 1.2, "unique_visitors": 450,  "bounce_rate_pct": 73.2},
    {"query": "serverless GPU compute",           "avg_time_on_page_sec": 155, "conversion_rate_pct": 3.9, "unique_visitors": 5200, "bounce_rate_pct": 31.8},
    {"query": "cloudflare workers vs lambda",     "avg_time_on_page_sec": 201, "conversion_rate_pct": 5.1, "unique_visitors": 3800, "bounce_rate_pct": 20.5},
    {"query": "edge AI model serving",            "avg_time_on_page_sec": 120, "conversion_rate_pct": 2.5, "unique_visitors": 900,  "bounce_rate_pct": 48.9},
]


# ------------------------------------------------------------------ #
#  Public API                                                         #
# ------------------------------------------------------------------ #

def fetch_semrush_gaps(
    api_key: Optional[str] = None,
    target_domain: str = "developers.cloudflare.com",
    competitor_domain: str = "aws.amazon.com/lambda",
) -> pd.DataFrame:
    """Return a DataFrame of keyword-gap rows where Cloudflare
    ranks lower (higher position number) than AWS Lambda.

    Parameters
    ----------
    api_key : str, optional
        SEMrush API key.  When *None*, simulated data is returned.
    target_domain : str
        The Cloudflare property to analyze.
    competitor_domain : str
        The competitor property for gap comparison.

    Returns
    -------
    pd.DataFrame
        Columns: query, cf_position, aws_position, search_volume,
        keyword_difficulty, position_gap
    """
    if api_key and api_key != "YOUR_SEMRUSH_API_KEY":
        # -----------------------------------------------------------
        # Production path — replace with real requests.get() call
        # pointing at the SEMrush Keyword Gap endpoint.
        # -----------------------------------------------------------
        logger.info("Fetching live SEMrush keyword-gap data for %s vs %s", target_domain, competitor_domain)
        raise NotImplementedError(
            "Live SEMrush integration pending.  "
            "Provide a valid API key and implement the HTTP call."
        )

    logger.info("Using simulated SEMrush keyword-gap data (%d rows)", len(_SIMULATED_SEMRUSH_DATA))
    df = pd.DataFrame(_SIMULATED_SEMRUSH_DATA)

    # Only keep queries where Cloudflare trails (higher position = worse rank)
    df = df[df["cf_position"] > df["aws_position"]].copy()
    df["position_gap"] = df["cf_position"] - df["aws_position"]
    df.sort_values("position_gap", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info("Keyword-gap analysis returned %d queries", len(df))
    return df


def fetch_adobe_traffic(
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    queries: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Return a DataFrame of historical engagement metrics for a
    list of search queries.

    Parameters
    ----------
    client_id : str, optional
        Adobe Analytics OAuth client ID.
    client_secret : str, optional
        Adobe Analytics OAuth client secret.
    queries : list[str], optional
        Limit results to these queries.  If *None*, return all
        available data.

    Returns
    -------
    pd.DataFrame
        Columns: query, avg_time_on_page_sec, conversion_rate_pct
    """
    if client_id and client_id != "YOUR_ADOBE_CLIENT_ID":
        logger.info("Fetching live Adobe Analytics traffic data")
        raise NotImplementedError(
            "Live Adobe Analytics integration pending.  "
            "Provide valid OAuth credentials and implement the HTTP call."
        )

    logger.info("Using simulated Adobe Analytics traffic data")
    df = pd.DataFrame(_SIMULATED_ADOBE_DATA)

    if queries is not None:
        df = df[df["query"].isin(queries)].copy()
        df.reset_index(drop=True, inplace=True)

    logger.info("Adobe traffic data returned %d rows", len(df))
    return df


def build_enriched_dataset(
    semrush_api_key: Optional[str] = None,
    adobe_client_id: Optional[str] = None,
    adobe_client_secret: Optional[str] = None,
) -> pd.DataFrame:
    """Orchestrate the full ingestion pipeline: pull keyword gaps,
    pull traffic metrics, and merge on *query*.

    Returns
    -------
    pd.DataFrame
        Enriched dataset with competitive-gap AND business-value
        columns for each query.
    """
    logger.info("=== Starting data ingestion pipeline ===")

    gap_df = fetch_semrush_gaps(api_key=semrush_api_key)
    traffic_df = fetch_adobe_traffic(
        client_id=adobe_client_id,
        client_secret=adobe_client_secret,
        queries=gap_df["query"].tolist(),
    )

    merged = pd.merge(gap_df, traffic_df, on="query", how="left")

    # Flag any queries that had no Adobe match
    missing_traffic = merged["avg_time_on_page_sec"].isna().sum()
    if missing_traffic:
        logger.warning(
            "%d queries had no matching Adobe traffic data — filling with 0",
            missing_traffic,
        )
        merged.fillna({
            "avg_time_on_page_sec": 0, 
            "conversion_rate_pct": 0.0,
            "unique_visitors": 0,
            "bounce_rate_pct": 100.0
        }, inplace=True)

    logger.info(
        "Enriched dataset built: %d rows × %d columns",
        len(merged),
        len(merged.columns),
    )
    return merged


# ------------------------------------------------------------------ #
#  CLI convenience                                                    #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    enriched = build_enriched_dataset()
    print("\n📊  Enriched Dataset:\n")
    print(enriched.to_string(index=False))
