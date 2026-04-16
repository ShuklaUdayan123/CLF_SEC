import os
import sys
import yaml
import logging
import argparse

# Ensure we can import from src no matter where the script is run from
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Import pipeline components
from src.fetchers.github import fetch_github_issues
from src.fetchers.stackoverflow import fetch_stackoverflow_questions
from src.fetchers.reddit import fetch_reddit_posts
from src.analyzers.relevance_classifier import classify_issue_relevance
from src.analyzers.sentiment_authority import analyze_sentiment_and_authority
from src.data_writer import save_analyzed_data

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    default_config = os.path.join(BASE_DIR, "config", "settings.yaml")
    parser = argparse.ArgumentParser(description="Cloudflare AI Issue Visibility Pipeline")
    parser.add_argument("--config", type=str, default=default_config, help="Path to config file")
    args = parser.parse_args()

    logger.info("Starting Cloudflare Issue ETL Pipeline...")
    
    if not os.path.exists(args.config):
        logger.error(f"Configuration file not found: {args.config}")
        return

    config = load_config(args.config)

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

    logger.info("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
