"""
Real xAI API Client for Event Extraction
Replace mock calls in the main extractor when ready for production
"""

import aiohttp
import asyncio
import json
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class XAIClient:
    """
    Real xAI API client for production use
    """

    def __init__(self, api_key: str, base_url: str = "https://api.x.ai/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def chat_completion(
        self,
        messages: list,
        model: str = "grok-4",
        functions: Optional[list] = None,
        function_call: Optional[Dict[str, Any]] = None,
        search_parameters: Optional[Dict[str, Any]] = None,
        temperature: float = 0.1,
        max_tokens: int = 4000
    ) -> Dict[str, Any]:
        """
        Make chat completion request with Live Search and Function Calling
        """

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        # Add function calling if specified
        if functions:
            payload["functions"] = functions
        if function_call:
            payload["function_call"] = function_call

        # Add Live Search parameters
        if search_parameters:
            payload["search_parameters"] = search_parameters

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"API call failed: {response.status} - {error_text}")

                result = await response.json()
                return result

        except Exception as e:
            logger.error(f"xAI API call failed: {e}")
            raise

    async def estimate_cost(
        self,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int = 0,
        search_sources: int = 0
    ) -> Dict[str, float]:
        """
        Estimate cost for API call
        """

        # Current xAI pricing (as of Sept 2025)
        input_cost = ((input_tokens - cached_tokens) * 3.00 + cached_tokens * 0.75) / 1_000_000
        output_cost = output_tokens * 15.00 / 1_000_000
        search_cost = search_sources * 0.025

        return {
            "input_cost": input_cost,
            "output_cost": output_cost,
            "search_cost": search_cost,
            "total_cost": input_cost + output_cost + search_cost
        }

# Example usage for integrating into the main extractor
async def example_real_api_usage():
    """
    Example of how to use the real API client
    Replace the mock _make_api_call method in OptimizedXAIEventExtractor
    """

    api_key = "your_xai_api_key_here"  # Replace with real key

    async with XAIClient(api_key) as client:

        messages = [
            {
                "role": "system",
                "content": "You are a financial event extraction system..."
            },
            {
                "role": "user",
                "content": "Extract earnings events from Sept 1-13, 2025 for AAPL, TSLA, MSFT"
            }
        ]

        functions = [{
            "name": "extract_financial_events",
            "description": "Extract financial events",
            "parameters": {
                "type": "object",
                "properties": {
                    "events": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "event_type": {"type": "string"},
                                "company_symbol": {"type": "string"},
                                "details": {"type": "string"},
                                "event_date": {"type": "string"},
                                "impact_level": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }]

        search_params = {
            "search_mode": "comprehensive",
            "max_search_results": 50
        }

        try:
            response = await client.chat_completion(
                messages=messages,
                functions=functions,
                function_call={"name": "extract_financial_events"},
                search_parameters=search_params
            )

            print("✅ API call successful")
            print(f"Response: {json.dumps(response, indent=2)}")

        except Exception as e:
            print(f"❌ API call failed: {e}")

if __name__ == "__main__":
    # Test the real API client (requires valid API key)
    asyncio.run(example_real_api_usage())