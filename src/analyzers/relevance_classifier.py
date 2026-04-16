import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Try to import openai, fallback gracefully if not installed
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

def classify_issue_relevance(issue: Dict[str, Any], config: dict) -> Dict[str, Any]:
    """
    Uses an LLM (via OpenAI API) to determine if a given developer issue could be 
    solved by a Cloudflare product.
    Returns the updated issue with 'relevance' attached.
    """
    llm_config = config.get("llm", {})
    provider = llm_config.get("provider", "openai")
    model = llm_config.get("model", "gpt-4o-mini")
    api_key = llm_config.get("api_key", "")
    
    prompt_template = config.get("prompts", {}).get("relevance", "")
    
    title = issue.get("title", "")
    body = issue.get("body", "")
    content_to_analyze = f"Title: {title}\nBody: {body}"
    
    # Mock fallback logic if key is missing or invalid
    if not HAS_OPENAI or api_key == "YOUR_LLM_API_KEY_HERE" or not api_key:
        logger.warning("OpenAI not configured properly, running relevance classifier in mock mode for issue: %s", title)
        # Mock simple keyword logic
        is_relevant = "CF" in title or "Cloudflare" in title or "R2" in body or "Workers" in title or "WAF" in body
        relevance_result = {
            "relevant": is_relevant,
            "confidence_score": 0.85,
            "reasoning": "Mocked response based on basic keyword heuristic."
        }
        issue["relevance"] = relevance_result
        return issue

    if provider == "openai":
        try:
            client = OpenAI(api_key=api_key)
            messages = [
                {"role": "system", "content": prompt_template},
                {"role": "user", "content": content_to_analyze}
            ]
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                response_format={"type": "json_object"}
            )
            raw_response = response.choices[0].message.content
            relevance_result = json.loads(raw_response)
            issue["relevance"] = relevance_result
            logger.info(f"Classified relevance for {title}: {relevance_result.get('relevant')}")
        except Exception as e:
            logger.error(f"Error classifying relevance with OpenAI: {e}")
            issue["relevance"] = {"relevant": False, "confidence_score": 0.0, "reasoning": f"Error: {e}"}
            
    else:
        logger.warning(f"Unsupported provider: {provider}")
        issue["relevance"] = {"relevant": False, "confidence_score": 0.0, "reasoning": "Unsupported provider"}

    return issue
