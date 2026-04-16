import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def fetch_reddit_posts(config: dict) -> List[Dict]:
    """
    Stub for fetching from Reddit.
    Returns mock data for the pipeline demonstration.
    """
    logger.warning("Reddit fetcher is running in mock mode.")
    return [
        {
            "source": "reddit",
            "id": "30001",
            "title": "Are R2 egress fees really zero?",
            "body": "I'm considering migrating from S3 to R2. S3 is killing me. Is R2 actually zero egress even if I don't use the CDN? See https://news.ycombinator.com/item?id=12345",
            "author": "redditor_xyz",
            "date": "2026-04-13T22:45:00Z",
            "url": "https://reddit.com/r/CloudFlare/comments/30001/"
        }
    ]
