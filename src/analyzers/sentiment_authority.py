import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

def analyze_sentiment_and_authority(issue: Dict[str, Any], config: dict) -> Dict[str, Any]:
    """
    Generates a simulated user prompt and predicts sentiment.
    Returns the issue with 'analysis' attached.
    """
    llm_config = config.get("llm", {})
    provider = llm_config.get("provider", "openai")
    model = llm_config.get("model", "gpt-4o-mini")
    api_key = llm_config.get("api_key", "")
    
    prompt_template = config.get("prompts", {}).get("sentiment_prompt_generation", "")
    
    title = issue.get("title", "")
    body = issue.get("body", "")
    content_to_analyze = f"Title: {title}\nBody: {body}"
    
    # Mock fallback logic
    if not HAS_OPENAI or api_key == "YOUR_LLM_API_KEY_HERE" or not api_key:
        logger.warning("OpenAI not configured properly, running sentiment analyzer in mock mode for issue: %s", title)
        analysis_result = {
            "simulated_user_prompt": f"How do I fix this issue regarding {title}?",
            "sentiment": {
                "cloudflare": "neutral",
                "general": "neutral"
            }
        }
        issue["analysis"] = analysis_result
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
            analysis_result = json.loads(raw_response)
            issue["analysis"] = analysis_result
            logger.info(f"Analyzed sentiment/authority for {title}")
        except Exception as e:
            logger.error(f"Error analyzing sentiment with OpenAI: {e}")
            issue["analysis"] = {
                "simulated_user_prompt": "",
                "sentiment": {"cloudflare": "unknown", "general": "unknown"}
            }
    else:
        issue["analysis"] = {
            "simulated_user_prompt": "",
            "sentiment": {"cloudflare": "unknown", "general": "unknown"}
        }

    return issue
