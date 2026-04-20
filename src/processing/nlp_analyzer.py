"""
nlp_analyzer.py — NLP Processing Layer
========================================
Provides two core capabilities:

1. **Sentiment Analysis** — Scores AI-generated answers for how
   favorably they mention Cloudflare (Positive / Neutral / Negative).
2. **Citation Extraction** — Parses free-text responses for external
   URLs and builds a "Priority Source List" DataFrame.

The sentiment scorer ships with a lightweight rule-based fallback
so the pipeline can run without an LLM key.  When a valid OpenAI
key is supplied, it delegates to GPT-4o for nuanced classification.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Sentiment keyword lexicon (rule-based fallback)                    #
# ------------------------------------------------------------------ #

_POSITIVE_SIGNALS: set[str] = {
    "fast", "faster", "fastest", "performant", "low latency",
    "scalable", "easy to use", "simple", "powerful", "innovative",
    "reliable", "cost-effective", "free tier", "global network",
    "developer-friendly", "recommended", "best", "superior",
    "efficient", "impressive", "leading", "excellent", "great",
    "advantage", "outperforms", "seamless",
}

_NEGATIVE_SIGNALS: set[str] = {
    "slow", "expensive", "limited", "lacking", "immature",
    "complex", "difficult", "unreliable", "downtime",
    "inferior", "behind", "worse", "missing", "poor",
    "disappointing", "restrictive", "buggy", "unstable",
    "not recommended", "outperformed by",
}


# ------------------------------------------------------------------ #
#  Public API                                                         #
# ------------------------------------------------------------------ #

def analyze_sentiment(
    text: str,
    subject: str = "Cloudflare",
    llm_api_key: Optional[str] = None,
    model: str = "gpt-4o",
) -> dict:
    """Score the sentiment of *text* with respect to *subject*.

    Parameters
    ----------
    text : str
        The AI-generated answer to evaluate.
    subject : str
        The brand or product to assess sentiment for.
    llm_api_key : str, optional
        If provided (and not a placeholder), use the OpenAI API.
        Otherwise, fall back to a deterministic keyword scorer.
    model : str
        OpenAI model identifier.

    Returns
    -------
    dict
        Keys: ``label`` (Positive | Neutral | Negative),
              ``confidence`` (float 0–1),
              ``method`` ("llm" | "rule-based"),
              ``detail`` (explanation string).
    """
    if llm_api_key and llm_api_key != "YOUR_OPENAI_API_KEY":
        return _llm_sentiment(text, subject, llm_api_key, model)
    return _rule_based_sentiment(text, subject)


def extract_citations(
    text: str,
    source_labels: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """Parse *text* for URLs and return a Priority Source List.

    Parameters
    ----------
    text : str
        Free-form text (e.g. an AI-generated answer) that may
        contain inline URLs.
    source_labels : dict, optional
        Mapping of domain substrings → human-readable labels.
        Defaults cover Reddit, StackOverflow, GitHub, HN, etc.

    Returns
    -------
    pd.DataFrame
        Columns: url, domain, source_label, priority
        Sorted by priority (1 = highest).
    """
    if source_labels is None:
        source_labels = {
            "reddit.com":        "Reddit",
            "stackoverflow.com": "StackOverflow",
            "stackexchange.com": "StackExchange",
            "github.com":        "GitHub",
            "news.ycombinator":  "Hacker News",
            "dev.to":            "Dev.to",
            "medium.com":        "Medium",
            "twitter.com":       "Twitter/X",
            "x.com":             "Twitter/X",
            "youtube.com":       "YouTube",
            "docs.aws.amazon":   "AWS Docs",
        }

    # Extract all URLs via regex
    url_pattern = re.compile(
        r"https?://[^\s\)\]\},\"\'<>]+", re.IGNORECASE
    )
    urls = url_pattern.findall(text)

    if not urls:
        logger.info("No citations found in the provided text")
        return pd.DataFrame(columns=["url", "domain", "source_label", "priority"])

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_urls: list[str] = []
    for u in urls:
        # Strip trailing punctuation that regex may capture
        u_clean = u.rstrip(".,;:!?")
        if u_clean not in seen:
            seen.add(u_clean)
            unique_urls.append(u_clean)

    records: list[dict] = []
    for url in unique_urls:
        domain = _extract_domain(url)
        label = "Other"
        for substr, lbl in source_labels.items():
            if substr in url.lower():
                label = lbl
                break
        records.append({"url": url, "domain": domain, "source_label": label})

    df = pd.DataFrame(records)

    # Assign priority: community sources first, official docs last
    priority_order = [
        "Reddit", "StackOverflow", "StackExchange", "Hacker News",
        "GitHub", "Dev.to", "Twitter/X", "YouTube", "Medium",
        "AWS Docs", "Other",
    ]
    priority_map = {label: idx + 1 for idx, label in enumerate(priority_order)}
    df["priority"] = df["source_label"].map(priority_map).fillna(len(priority_order) + 1).astype(int)
    df.sort_values("priority", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info("Extracted %d unique citations from text", len(df))
    return df


# ------------------------------------------------------------------ #
#  Internal helpers                                                   #
# ------------------------------------------------------------------ #

def _rule_based_sentiment(text: str, subject: str) -> dict:
    """Deterministic keyword-matching sentiment scorer."""
    text_lower = text.lower()

    # Only consider sentences that mention the subject
    sentences = re.split(r"[.!?]+", text_lower)
    relevant = [s for s in sentences if subject.lower() in s]

    if not relevant:
        return {
            "label": "Neutral",
            "confidence": 0.5,
            "method": "rule-based",
            "detail": f"No sentences mentioning '{subject}' were found.",
        }

    pos_hits = sum(1 for s in relevant for kw in _POSITIVE_SIGNALS if kw in s)
    neg_hits = sum(1 for s in relevant for kw in _NEGATIVE_SIGNALS if kw in s)
    total = pos_hits + neg_hits

    if total == 0:
        label, confidence = "Neutral", 0.5
    elif pos_hits > neg_hits:
        label = "Positive"
        confidence = round(pos_hits / total, 2)
    elif neg_hits > pos_hits:
        label = "Negative"
        confidence = round(neg_hits / total, 2)
    else:
        label, confidence = "Neutral", 0.5

    return {
        "label": label,
        "confidence": confidence,
        "method": "rule-based",
        "detail": f"pos_hits={pos_hits}, neg_hits={neg_hits} across {len(relevant)} relevant sentence(s).",
    }


def _llm_sentiment(
    text: str,
    subject: str,
    api_key: str,
    model: str,
) -> dict:
    """Delegate sentiment classification to an OpenAI chat model."""
    try:
        import openai  # type: ignore
    except ImportError as exc:
        logger.warning("openai package not installed — falling back to rule-based scorer")
        return _rule_based_sentiment(text, subject)

    client = openai.OpenAI(api_key=api_key)

    system_prompt = (
        f"You are a brand-sentiment analyst.  Given a text, classify the overall "
        f"sentiment TOWARD '{subject}' as exactly one of: Positive, Neutral, Negative.  "
        f"Respond with a JSON object: {{\"label\": \"...\", \"confidence\": 0.0–1.0, \"detail\": \"...\"}}."
    )

    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.0,
            max_tokens=256,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
            response_format={"type": "json_object"},
        )
        import json
        result = json.loads(response.choices[0].message.content)
        result["method"] = "llm"
        return result

    except Exception as exc:  # noqa: BLE001
        logger.error("LLM sentiment call failed: %s — falling back to rule-based", exc)
        return _rule_based_sentiment(text, subject)


def _extract_domain(url: str) -> str:
    """Pull a clean domain from a URL string."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc or url
    except Exception:
        return url


# ------------------------------------------------------------------ #
#  CLI convenience                                                    #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    sample_answer = (
        "Cloudflare Workers is fast and developer-friendly for edge computing. "
        "However, AWS Lambda still outperforms it for long-running tasks. "
        "See discussions at https://www.reddit.com/r/cloudflare/comments/example "
        "and https://stackoverflow.com/questions/12345/serverless-comparison. "
        "AWS documentation is at https://docs.aws.amazon.com/lambda/latest/dg/. "
        "Also check https://github.com/cloudflare/workers-sdk for the SDK."
    )

    print("🧠  Sentiment Analysis:")
    sentiment = analyze_sentiment(sample_answer)
    for k, v in sentiment.items():
        print(f"   {k}: {v}")

    print("\n🔗  Extracted Citations:")
    citations = extract_citations(sample_answer)
    print(citations.to_string(index=False))
