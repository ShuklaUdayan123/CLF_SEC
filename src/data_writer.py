import os
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

def save_analyzed_data(data: List[Dict[str, Any]], config: dict) -> None:
    """
    Saves the analyzed issues into a structured CSV and priority sources mapping
    into a JSON file using pandas (if available).
    """
    storage_config = config.get("storage", {})
    output_dir = storage_config.get("output_dir", "./data")
    csv_file = storage_config.get("processed_issues_file", "processed_issues.csv")
    json_file = storage_config.get("simulated_prompts_file", "simulated_prompts.json")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    csv_path = os.path.join(output_dir, csv_file)
    json_path = os.path.join(output_dir, json_file)

    if not data:
        logger.warning("No data provided to save.")
        return

    # Extract all simulated user prompts across all issues
    all_prompts = []
    for item in data:
        analysis = item.get("analysis", {})
        if analysis and analysis.get("simulated_user_prompt"):
            all_prompts.append({
                "source_issue_id": item.get("id"),
                "source_platform": item.get("source"),
                "simulated_user_prompt": analysis.get("simulated_user_prompt"),
                "cloudflare_sentiment": analysis.get("sentiment", {}).get("cloudflare", "unknown")
            })

    if HAS_PANDAS:
        # Save structural issues to CSV
        issues_df = pd.json_normalize(data)
        issues_df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(issues_df)} parsed issues to {csv_path}")

        # Save simulated prompts mapped to JSON
        with open(json_path, 'w') as f:
            json.dump(all_prompts, f, indent=2)
        logger.info(f"Saved {len(all_prompts)} simulated prompts to {json_path}")
    else:
        logger.warning("Pandas not found. Using standard JSON serialization for all output.")
        fallback_path = os.path.join(output_dir, "processed_issues_raw.json")
        with open(fallback_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        with open(json_path, 'w') as f:
            json.dump(all_prompts, f, indent=2)
        logger.info(f"Saved raw data to {fallback_path} and {json_path}")
