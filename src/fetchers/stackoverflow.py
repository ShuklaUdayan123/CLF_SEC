import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def fetch_stackoverflow_questions(config: dict) -> List[Dict]:
    """
    Stub for fetching from StackOverflow.
    Returns mock data for the pipeline demonstration.
    """
    logger.warning("StackOverflow fetcher is running in mock mode.")
    return [
        {
            "source": "stackoverflow",
            "id": "20001",
            "title": "Cloudflare Workers KV eventual consistency issue",
            "body": "I am writing a blog platform and storing post metadata in KV. Sometimes it takes up to 60 seconds to update globally. I am reading this post: https://example.com/cloudflare-kv-limits",
            "author": "so_user_99",
            "date": "2026-04-14T09:12:00Z",
            "url": "https://stackoverflow.com/questions/20001/cf-kv-issue"
        }
    ]
