import os
import sys
import yaml
import logging
import argparse
import pandas as pd

# Ensure we can import from src no matter where the script is run from
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Import issue pipeline components
from src.fetchers.github import fetch_github_issues
from src.fetchers.stackoverflow import fetch_stackoverflow_questions
from src.fetchers.reddit import fetch_reddit_posts
from src.analyzers.relevance_classifier import classify_issue_relevance
from src.analyzers.sentiment_authority import analyze_sentiment_and_authority
from src.data_writer import save_analyzed_data

# Import AI visibility pipeline components
from src.ingestion.data_extractor import build_enriched_dataset
from src.processing.impact_model import calculate_priority_score
from src.processing.nlp_analyzer import analyze_sentiment, extract_citations
from src.reporting.executive_summary import generate_report

def load_config(config_path: str) -> dict:
    with open(config_path, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file)

def run_issues_pipeline(config):
    logger.info("Starting Cloudflare Community Issues ETL Pipeline...")
    
    # 1. Fetching
    logger.info("=== Phase 1: Fetching issues ===")
    issues = []
    issues.extend(fetch_github_issues(config))
    issues.extend(fetch_stackoverflow_questions(config))
    issues.extend(fetch_reddit_posts(config))
    logger.info(f"Total issues fetched initially: {len(issues)}")

    # 2. Relevance Classification
    logger.info("=== Phase 2: Relevance Classification ===")
    classified_issues = []
    for issue in issues:
        processed = classify_issue_relevance(issue, config)
        classified_issues.append(processed)
        
    # Filter for only relevant issues
    relevant_issues = [
        issue for issue in classified_issues 
        if issue.get("relevance", {}).get("relevant") == True
    ]
    logger.info(f"Filtered {len(relevant_issues)} out of {len(issues)} as relevant for Cloudflare.")

    # 3. Sentiment & Authority Analysis
    logger.info("=== Phase 3: Sentiment & Authority Map ===")
    fully_analyzed_issues = []
    for issue in relevant_issues:
        analyzed = analyze_sentiment_and_authority(issue, config)
        fully_analyzed_issues.append(analyzed)

    # 4. Data Writing
    logger.info("=== Phase 4: Storing Results ===")
    save_analyzed_data(fully_analyzed_issues, config)

    logger.info("Community Issues Pipeline completed successfully!")

def run_visibility_pipeline(config):
    logger.info("Starting Cloudflare AI Visibility Pipeline...")

    # Phase 1: Ingestion
    logger.info("=== Phase 1: Data Ingestion ===")
    enriched_df = build_enriched_dataset(
        semrush_api_key=config.get("semrush", {}).get("api_key"),
        adobe_client_id=config.get("adobe_analytics", {}).get("client_id"),
        adobe_client_secret=config.get("adobe_analytics", {}).get("client_secret"),
    )

    # Phase 2: NLP Analysis
    logger.info("=== Phase 2: NLP Analysis (Batch) ===")
    
    simulated_answers = {
        "serverless edge computing": "Cloudflare Workers is fast, but AWS Lambda has a massive ecosystem.",
        "workers AI inference": "Workers AI is incredibly fast and performant for edge processing.",
        "deploy machine learning at edge": "Deploying machine learning can be difficult on Cloudflare. AWS is recommended.",
        "AI gateway API management": "Cloudflare's AI Gateway is a reliable, developer-friendly tool.",
        "edge function cold start": "Cloudflare Workers has virtually zero cold starts, outperforming Lambda.",
        "vector database serverless": "Vector databases are okay on Cloudflare but missing key features.",
        "LLM inference latency benchmark": "Cloudflare outperforms in low latency benchmarks.",
        "serverless GPU compute": "AWS offers far superior GPU compute. Cloudflare is restricted here.",
        "cloudflare workers vs lambda": "Cloudflare Workers is fast and developer-friendly for edge computing. However, AWS Lambda still outperforms it for long-running tasks. See discussions at https://www.reddit.com/r/cloudflare/comments/example and https://stackoverflow.com/questions/12345/serverless-comparison. AWS documentation is at https://docs.aws.amazon.com/lambda/latest/dg/. Also check https://github.com/cloudflare/workers-sdk for the SDK.",
        "edge AI model serving": "Cloudflare is excellent for serving models seamlessly."
    }

    sentiment_labels = []
    citation_dfs = []

    for query in enriched_df["query"]:
        ans = simulated_answers.get(query, f"Cloudflare is okay for {query}.")
        sent = analyze_sentiment(ans, llm_api_key=config.get("llm", {}).get("api_key"))
        sentiment_labels.append(sent["label"])
        
        cit_df = extract_citations(ans)
        citation_dfs.append(cit_df)

    enriched_df["ai_sentiment"] = sentiment_labels

    if citation_dfs:
        citations_df = pd.concat(citation_dfs, ignore_index=True)
        citations_df.drop_duplicates(subset=["url"], inplace=True)
        citations_df.sort_values("priority", inplace=True)
        citations_df.reset_index(drop=True, inplace=True)
    else:
        citations_df = pd.DataFrame()

    logger.info("Batch Sentiment Analysis completed.")
    logger.info(f"Citations extracted: {len(citations_df)} unique sources")

    # Phase 3: Prioritization
    logger.info("=== Phase 3: Prioritization Matrix ===")
    pipeline_cfg = config.get("pipeline", {})
    ranked_df = calculate_priority_score(
        enriched_df,
        quick_win_ease_min=pipeline_cfg.get("quick_win_ease_min", 7),
        quick_win_reach_min=pipeline_cfg.get("quick_win_reach_min", 5),
    )

    # Phase 4: Reporting
    logger.info("=== Phase 4: Executive Report Generation ===")
    report_path = generate_report(
        ranked_df,
        citations_df=citations_df,
        output_dir=pipeline_cfg.get("output_dir", "./output"),
        filename=pipeline_cfg.get("report_filename", "executive_report.md"),
    )

    logger.info(f"AI Visibility Pipeline complete — report at: {report_path}")

def main():
    default_config = os.path.join(BASE_DIR, "config", "settings.yaml")
    parser = argparse.ArgumentParser(description="Cloudflare Unified Intelligence Pipeline")
    parser.add_argument("--config", type=str, default=default_config, help="Path to config file")
    parser.add_argument("--run-issues", action="store_true", help="Run the Community Issues Pipeline")
    parser.add_argument("--run-visibility", action="store_true", help="Run the AI Visibility Pipeline")
    parser.add_argument("--run-all", action="store_true", help="Run both pipelines sequentially (default if no flags are provided)")
    args = parser.parse_args()

    # If no flags provided, default to --run-all
    if not (args.run_issues or args.run_visibility or args.run_all):
        args.run_all = True

    if not os.path.exists(args.config):
        logger.error(f"Configuration file not found: {args.config}")
        return

    config = load_config(args.config)

    if args.run_all or args.run_issues:
        run_issues_pipeline(config)
        
    if args.run_all or args.run_visibility:
        run_visibility_pipeline(config)

if __name__ == "__main__":
    main()
