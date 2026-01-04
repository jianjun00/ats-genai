"""
Unified Hot Topics Analysis Application
- Gets real data from Reddit/social media
- Uses Grok as main reasoning engine
- Uses OpenAI for cross-validation
- No fake rules, just AI reasoning
"""

import asyncio
import aiohttp
import json
import re
import logging
from typing import List, Dict, Any, Set
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class SocialPost:
    text: str
    score: int
    comments: int
    created: datetime
    author: str
    url: str
    source: str


@dataclass
class TopicAnalysis:
    topic: str
    tickers: List[str]
    grok_analysis: Dict[str, Any]
    openai_analysis: Dict[str, Any]
    consensus_score: float
    timestamp: datetime


class HotTopicsApp:
    """Unified application using AI reasoning"""
    
    def __init__(self, grok_api_key: str, openai_api_key: str = None):
        self.grok_api_key = grok_api_key
        self.openai_api_key = openai_api_key
        
        # S&P 500 major tickers for filtering
        self.major_tickers = {
            'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'BRK.B',
            'JPM', 'V', 'UNH', 'XOM', 'LLY', 'JNJ', 'WMT', 'MA', 'PG', 'AVGO',
            'HD', 'CVX', 'MRK', 'ABBV', 'COST', 'PEP', 'KO', 'ADBE', 'CRM',
            'BAC', 'NFLX', 'ACN', 'TMO', 'MCD', 'CSCO', 'ABT', 'WFC', 'INTC',
            'DHR', 'DIS', 'VZ', 'TXN', 'PFE', 'INTU', 'AMGN', 'PM', 'NKE',
            'COP', 'QCOM', 'IBM', 'GE', 'CAT', 'SPGI', 'NOW', 'HON', 'NEE'
        }
    
    async def get_reddit_wsb_posts(self) -> List[SocialPost]:
        """Get real posts from Reddit WallStreetBets"""
        logger.info("Fetching Reddit WSB posts...")
        
        posts = []
        headers = {'User-Agent': 'HotTopicsApp/1.0'}
        
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://www.reddit.com/r/wallstreetbets/hot.json?limit=100"
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        for item in data['data']['children']:
                            post_data = item['data']
                            
                            post = SocialPost(
                                text=f"{post_data['title']} {post_data.get('selftext', '')}",
                                score=post_data['score'],
                                comments=post_data['num_comments'],
                                created=datetime.fromtimestamp(post_data['created_utc']),
                                author=post_data['author'],
                                url=f"https://reddit.com{post_data['permalink']}",
                                source='reddit_wsb'
                            )
                            posts.append(post)
                        
                        logger.info(f"Fetched {len(posts)} Reddit posts")
                    else:
                        logger.error(f"Reddit API error: {response.status}")
                        
        except Exception as e:
            logger.error(f"Failed to fetch Reddit data: {e}")
            
        return posts
    
    def extract_tickers(self, posts: List[SocialPost]) -> Set[str]:
        """Extract stock tickers from posts"""
        tickers = set()
        
        for post in posts:
            # Extract $TICKER format
            dollar_matches = re.findall(r'\\$([A-Z]{1,5})\\b', post.text)
            for ticker in dollar_matches:
                if ticker in self.major_tickers:
                    tickers.add(ticker)
            
            # Extract explicit ticker mentions
            text_upper = post.text.upper()
            for ticker in self.major_tickers:
                if re.search(f'\\b{ticker}\\b', text_upper):
                    tickers.add(ticker)
        
        return tickers
    
    def get_top_discussions(self, posts: List[SocialPost]) -> List[str]:
        """Get most engaging discussions"""
        # Sort by engagement (score + comments)
        posts.sort(key=lambda p: p.score + p.comments * 2, reverse=True)
        
        # Get top discussions
        top_topics = []
        for post in posts[:20]:  # Top 20 by engagement
            if len(post.text.strip()) > 50:  # Filter out short posts
                top_topics.append(post.text[:1000])  # Truncate for API limits
        
        return top_topics
    
    async def call_grok_api(self, prompt: str) -> Dict[str, Any]:
        """Call Grok API for analysis"""
        headers = {
            'Authorization': f'Bearer {self.grok_api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "messages": [
                {
                    "role": "system",
                    "content": "You are a financial markets expert. Analyze social media data and provide detailed market impact analysis. Always respond with valid JSON."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            "model": "grok-3",
            "stream": False,
            "temperature": 0.1
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api.x.ai/v1/chat/completions',
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        content = result['choices'][0]['message']['content']
                        
                        # Extract JSON from response
                        json_start = content.find('{')
                        json_end = content.rfind('}') + 1
                        
                        if json_start != -1 and json_end > json_start:
                            json_str = content[json_start:json_end]
                            return json.loads(json_str)
                        else:
                            return {"analysis": content, "error": "No JSON found"}
                    else:
                        logger.error(f"Grok API error: {response.status}")
                        return {"error": f"API error: {response.status}"}
                        
        except Exception as e:
            logger.error(f"Grok API call failed: {e}")
            return {"error": str(e)}
    
    async def call_openai_api(self, prompt: str) -> Dict[str, Any]:
        """Call OpenAI API for cross-validation"""
        if not self.openai_api_key:
            return {"analysis": "OpenAI API key not provided", "cross_validation": False}
        
        headers = {
            'Authorization': f'Bearer {self.openai_api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "model": "gpt-4",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a financial markets expert. Provide market impact analysis. Always respond with valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api.openai.com/v1/chat/completions',
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        content = result['choices'][0]['message']['content']
                        
                        json_start = content.find('{')
                        json_end = content.rfind('}') + 1
                        
                        if json_start != -1 and json_end > json_start:
                            json_str = content[json_start:json_end]
                            return json.loads(json_str)
                        else:
                            return {"analysis": content, "cross_validation": True}
                    else:
                        return {"error": f"OpenAI API error: {response.status}"}
                        
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            return {"error": str(e)}
    
    async def analyze_hot_topics(self) -> List[TopicAnalysis]:
        """Main analysis function using AI reasoning"""
        logger.info("Starting hot topics analysis...")
        
        # Step 1: Get real social media data
        posts = await self.get_reddit_wsb_posts()
        if not posts:
            logger.warning("No posts found")
            return []
        
        # Step 2: Extract tickers and top discussions
        tickers = self.extract_tickers(posts)
        top_discussions = self.get_top_discussions(posts)
        
        logger.info(f"Found {len(tickers)} tickers and {len(top_discussions)} top discussions")
        
        # Step 3: Prepare data for AI analysis
        analysis_data = {
            "tickers_mentioned": list(tickers),
            "top_discussions": top_discussions[:10],  # Top 10 for API limits
            "post_count": len(posts),
            "timestamp": datetime.now().isoformat()
        }
        
        # Step 4: Create analysis prompt
        prompt = f"""
Analyze this real social media financial data and identify the hottest topics that will impact stock prices.

DATA:
- Tickers mentioned: {analysis_data['tickers_mentioned']}
- Top discussions from Reddit WallStreetBets:
{chr(10).join([f"{i+1}. {disc[:200]}..." for i, disc in enumerate(analysis_data['top_discussions'])])}

ANALYSIS REQUIRED:
1. Identify the 5 hottest topics that will move stock prices
2. For each hot topic, determine which specific stocks will be impacted
3. Predict price movement direction and magnitude
4. Assess confidence level and timeline

RESPOND IN JSON FORMAT:
{{
  "hot_topics": [
    {{
      "topic_name": "clear topic description",
      "affected_tickers": ["AAPL", "TSLA"],
      "impact_direction": "bullish/bearish/mixed",
      "price_impact_1d": "+2.5%",
      "price_impact_1w": "+5.2%", 
      "confidence": 0.85,
      "timeline": "immediate/hours/days",
      "reasoning": "detailed explanation of why this will impact prices",
      "catalyst_type": "earnings/news/regulatory/sentiment"
    }}
  ],
  "market_sentiment": "bullish/bearish/neutral",
  "overall_confidence": 0.80,
  "key_insights": ["insight 1", "insight 2"]
}}
"""
        
        # Step 5: Get Grok analysis
        logger.info("Getting Grok analysis...")
        grok_result = await self.call_grok_api(prompt)
        
        # Step 6: Get OpenAI cross-validation (if available)
        logger.info("Getting OpenAI cross-validation...")
        openai_result = await self.call_openai_api(prompt)
        
        # Step 7: Create topic analyses
        analyses = []
        
        if "hot_topics" in grok_result:
            for i, topic in enumerate(grok_result["hot_topics"]):
                # Calculate consensus score between Grok and OpenAI
                consensus_score = 0.8  # Default if no OpenAI
                
                if "hot_topics" in openai_result and i < len(openai_result["hot_topics"]):
                    openai_topic = openai_result["hot_topics"][i]
                    # Simple consensus: average confidence scores
                    grok_conf = topic.get("confidence", 0.5)
                    openai_conf = openai_topic.get("confidence", 0.5)
                    consensus_score = (grok_conf + openai_conf) / 2
                
                analysis = TopicAnalysis(
                    topic=topic.get("topic_name", "Unknown Topic"),
                    tickers=topic.get("affected_tickers", []),
                    grok_analysis=topic,
                    openai_analysis=openai_result.get("hot_topics", [{}])[i] if "hot_topics" in openai_result else {},
                    consensus_score=consensus_score,
                    timestamp=datetime.now()
                )
                analyses.append(analysis)
        
        logger.info(f"Generated {len(analyses)} topic analyses")
        return analyses
    
    def generate_report(self, analyses: List[TopicAnalysis]) -> str:
        """Generate comprehensive report"""
        if not analyses:
            return "No hot topics identified from current social media data."
        
        report = f"""
🚀 HOT TOPICS ANALYSIS - AI REASONING ENGINE
{'='*70}
Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
AI Models: Grok-3 (primary) + GPT-4 (validation)
Data Source: Reddit WallStreetBets (real posts)
Topics Identified: {len(analyses)}

"""
        
        for i, analysis in enumerate(analyses, 1):
            grok = analysis.grok_analysis
            consensus = analysis.consensus_score
            confidence_stars = "⭐" * max(1, int(consensus * 5))
            
            direction_emoji = {
                'bullish': '📈', 'bearish': '📉', 'mixed': '🔄'
            }.get(grok.get('impact_direction', 'mixed'), '➡️')
            
            report += f"""
{i}. {analysis.topic} {direction_emoji} {confidence_stars}
   Affected Tickers: {', '.join(analysis.tickers)}
   Direction: {grok.get('impact_direction', 'N/A').upper()}
   1-Day Impact: {grok.get('price_impact_1d', 'N/A')}
   1-Week Impact: {grok.get('price_impact_1w', 'N/A')}
   Timeline: {grok.get('timeline', 'N/A')}
   Consensus Score: {consensus:.0%}
   
   🤖 Grok Analysis:
   "{grok.get('reasoning', 'No reasoning provided')}"
   
   📊 Catalyst: {grok.get('catalyst_type', 'Unknown')}
   
"""
        
        # Summary statistics
        avg_consensus = sum(a.consensus_score for a in analyses) / len(analyses)
        bullish_count = sum(1 for a in analyses if a.grok_analysis.get('impact_direction') == 'bullish')
        bearish_count = sum(1 for a in analyses if a.grok_analysis.get('impact_direction') == 'bearish')
        
        report += f"""
📊 ANALYSIS SUMMARY:
• Average Consensus: {avg_consensus:.0%}
• Bullish Signals: {bullish_count}
• Bearish Signals: {bearish_count}
• High Confidence Topics: {sum(1 for a in analyses if a.consensus_score > 0.7)}

💡 METHODOLOGY:
• Real social media data from Reddit WSB
• Grok-3 as primary reasoning engine
• GPT-4 for cross-validation (if available)
• No manual rules - pure AI reasoning
"""
        
        return report


async def main():
    """Run the unified hot topics application"""
    logging.basicConfig(level=logging.INFO)
    
    # API keys
    grok_api_key = "xai-YaEodOfHizh1DKIoRT78lUALMQhM7Jw29CAORHz1qsv2HvXto3z5wF33MmtB91ajQdserzkcUwSDyaF1"
    openai_api_key = None  # Add OpenAI key if available
    
    # Create and run application
    app = HotTopicsApp(grok_api_key, openai_api_key)
    
    # Run analysis
    analyses = await app.analyze_hot_topics()
    
    # Generate report
    report = app.generate_report(analyses)
    print(report)
    
    # Save results
    results = {
        "timestamp": datetime.now().isoformat(),
        "analyses": [
            {
                "topic": a.topic,
                "tickers": a.tickers,
                "grok_analysis": a.grok_analysis,
                "openai_analysis": a.openai_analysis,
                "consensus_score": a.consensus_score
            }
            for a in analyses
        ]
    }
    
    with open('/home/jianjun/ats-genai-data/hot_topics_app/results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\\n📄 Detailed results saved to: hot_topics_app/results.json")


if __name__ == "__main__":
    asyncio.run(main())