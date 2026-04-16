import requests
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def fetch_github_issues(config: dict) -> List[Dict]:
    """
    Fetches recent issues from a target GitHub repository.
    Handles basic rate-limiting/mocking.
    """
    github_config = config.get("apis", {}).get("github", {})
    endpoint = github_config.get("endpoint", "https://api.github.com")
    token = github_config.get("token", "")
    target_repo = github_config.get("target_repo", "cloudflare/cloudflare-go")
    
    if token == "YOUR_GITHUB_TOKEN_HERE" or not token:
        logger.warning("GitHub token not set. Using mock local data for demonstration.")
        return get_mock_data()

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {token}"
    }

    url = f"{endpoint}/repos/{target_repo}/issues"
    
    try:
        # Fetch up to 10 issues for the pipeline demo
        response = requests.get(url, headers=headers, params={"state": "open", "per_page": 10}, timeout=10)
        response.raise_for_status()
        
        issues = response.json()
        structured_data = []
        for issue in issues:
            # Skip pull requests
            if "pull_request" in issue:
                continue
                
            structured_data.append({
                "source": "github",
                "id": str(issue.get("id")),
                "title": issue.get("title", ""),
                "body": issue.get("body", "") or "",
                "author": issue.get("user", {}).get("login", "unknown"),
                "date": issue.get("created_at"),
                "url": issue.get("html_url")
            })
        
        logger.info(f"Fetched {len(structured_data)} issues from GitHub ({target_repo}).")
        return structured_data
        
    except requests.RequestException as e:
        logger.error(f"Error fetching from GitHub: {e}")
        return get_mock_data()

def get_mock_data() -> List[Dict]:
    return [
        {
            "source": "github",
            "id": "10001",
            "title": "Rate limiting documentation is confusing",
            "body": "I'm trying to implement advanced rate limiting via Workers but the examples at https://developers.cloudflare.com/workers don't show how to handle distributed state properly.",
            "author": "mock_user_1",
            "date": "2026-04-16T10:00:00Z",
            "url": "https://github.com/mock/repo/issues/1"
        },
        {
            "source": "github",
            "id": "10002",
            "title": "Need a way to bypass WAF for specific IPs dynamically",
            "body": "Is there a way to update IP lists for the WAF without redeploying via Terraform? Our CI/CD is too slow.",
            "author": "mock_user_2",
            "date": "2026-04-15T14:30:00Z",
            "url": "https://github.com/mock/repo/issues/2"
        }
    ]
