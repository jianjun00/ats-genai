"""
Intelligent Routing System for LLM Pilot

This module implements intelligent routing between DeepSeek, Llama (future), and FinBERT
based on content complexity, importance, and cost optimization considerations.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

from sentiment.news_sentiment_analyzer import NewsArticle

logger = logging.getLogger(__name__)


@dataclass
class RoutingDecision:
    """Structured routing decision with reasoning"""
    processor: str  # 'deepseek', 'llama', 'finbert'
    confidence: float  # 0.0 to 1.0
    reasoning: str
    priority: int  # 1=high, 2=medium, 3=low
    expected_cost: float
    expected_latency: float


class NewsContentAnalyzer:
    """
    Analyze news content to determine complexity and importance for routing decisions.
    """
    
    def __init__(self):
        # High-impact financial keywords that warrant deep LLM analysis
        self.high_impact_keywords = {
            # Earnings and financials
            'earnings', 'revenue', 'profit', 'loss', 'guidance', 'outlook',
            'eps', 'beat', 'miss', 'consensus', 'estimate', 'forecast',
            
            # Corporate actions
            'merger', 'acquisition', 'buyout', 'takeover', 'deal', 'transaction',
            'dividend', 'buyback', 'repurchase', 'split', 'spinoff',
            'restructuring', 'bankruptcy', 'chapter 11',
            
            # Regulatory and clinical
            'fda', 'approval', 'clinical trial', 'phase', 'study', 'drug',
            'regulatory', 'sec', 'investigation', 'lawsuit', 'settlement',
            
            # Strategic business
            'partnership', 'joint venture', 'collaboration', 'licensing',
            'ipo', 'listing', 'delisting', 'private equity',
            'expansion', 'growth', 'investment', 'funding'
        }
        
        # Medium-impact keywords for moderate analysis
        self.medium_impact_keywords = {
            'upgrade', 'downgrade', 'analyst', 'rating', 'price target',
            'management', 'ceo', 'cfo', 'executive', 'board',
            'product', 'launch', 'innovation', 'technology',
            'market share', 'competition', 'competitor'
        }
        
        # Complexity indicators
        self.complexity_indicators = {
            'multiple_numbers',  # Article contains many numerical values
            'technical_language',  # Contains technical/financial jargon
            'multiple_companies',  # Mentions multiple companies
            'regulatory_content',  # Contains regulatory language
            'forward_looking',  # Contains forward-looking statements
            'contradictory_info'  # Contains conflicting information
        }
        
    def analyze_content(self, article: NewsArticle) -> Dict[str, any]:
        """
        Analyze article content to determine routing requirements.
        
        Returns:
            Dict with analysis results including complexity, importance, etc.
        """
        
        full_text = f"{article.title} {article.content}".lower()
        
        analysis = {
            'complexity_score': self._calculate_complexity_score(full_text),
            'importance_score': self._calculate_importance_score(full_text),
            'numerical_density': self._calculate_numerical_density(full_text),
            'technical_complexity': self._assess_technical_complexity(full_text),
            'multi_company_impact': self._detect_multi_company_impact(full_text),
            'time_sensitivity': self._assess_time_sensitivity(article),
            'content_length': len(full_text),
            'keyword_matches': self._find_keyword_matches(full_text)
        }
        
        return analysis
    
    def _calculate_complexity_score(self, text: str) -> float:
        """Calculate content complexity score (0.0 to 1.0)"""
        
        score = 0.0
        
        # Length-based complexity
        if len(text) > 2000:
            score += 0.2
        elif len(text) > 1000:
            score += 0.1
        
        # Numerical complexity
        numbers = re.findall(r'\d+(?:\.\d+)?', text)
        if len(numbers) > 10:
            score += 0.3
        elif len(numbers) > 5:
            score += 0.15
        
        # Financial terminology density
        financial_terms = [
            'revenue', 'ebitda', 'margin', 'ratio', 'valuation', 
            'multiple', 'yield', 'volatility', 'correlation'
        ]
        term_count = sum(1 for term in financial_terms if term in text)
        score += min(term_count * 0.05, 0.2)
        
        # Sentence complexity (long sentences = higher complexity)
        sentences = text.split('.')
        avg_sentence_length = sum(len(s.split()) for s in sentences) / len(sentences)
        if avg_sentence_length > 25:
            score += 0.15
        elif avg_sentence_length > 20:
            score += 0.1
        
        return min(score, 1.0)
    
    def _calculate_importance_score(self, text: str) -> float:
        """Calculate content importance score (0.0 to 1.0)"""
        
        score = 0.0
        
        # High-impact keyword presence
        high_matches = sum(1 for keyword in self.high_impact_keywords if keyword in text)
        score += min(high_matches * 0.15, 0.6)
        
        # Medium-impact keyword presence
        medium_matches = sum(1 for keyword in self.medium_impact_keywords if keyword in text)
        score += min(medium_matches * 0.05, 0.2)
        
        # Quantified impact indicators
        if any(pattern in text for pattern in ['%', 'percent', 'billion', 'million']):
            score += 0.1
        
        # Urgency indicators
        urgency_words = ['breaking', 'urgent', 'immediate', 'emergency', 'halt']
        if any(word in text for word in urgency_words):
            score += 0.15
        
        return min(score, 1.0)
    
    def _calculate_numerical_density(self, text: str) -> float:
        """Calculate density of numerical information"""
        
        words = text.split()
        if len(words) == 0:
            return 0.0
        
        # Count numbers, percentages, currency amounts
        numerical_patterns = [
            r'\d+(?:\.\d+)?',  # Regular numbers
            r'\$\d+(?:\.\d+)?[bmk]?',  # Currency
            r'\d+(?:\.\d+)?%',  # Percentages
        ]
        
        numerical_count = 0
        for pattern in numerical_patterns:
            numerical_count += len(re.findall(pattern, text))
        
        return numerical_count / len(words)
    
    def _assess_technical_complexity(self, text: str) -> float:
        """Assess technical/financial language complexity"""
        
        technical_terms = {
            'derivative', 'option', 'warrant', 'convertible', 'subordinated',
            'amortization', 'depreciation', 'impairment', 'goodwill',
            'covenant', 'collateral', 'syndicated', 'leveraged',
            'hedging', 'arbitrage', 'correlation', 'volatility'
        }
        
        matches = sum(1 for term in technical_terms if term in text)
        return min(matches * 0.1, 1.0)
    
    def _detect_multi_company_impact(self, text: str) -> bool:
        """Detect if article affects multiple companies"""
        
        # Look for patterns indicating multiple company impact
        multi_company_indicators = [
            'sector', 'industry', 'peers', 'competitors', 'suppliers',
            'customers', 'partners', 'ecosystem', 'supply chain'
        ]
        
        return any(indicator in text for indicator in multi_company_indicators)
    
    def _assess_time_sensitivity(self, article: NewsArticle) -> float:
        """Assess time sensitivity based on publication timing and content"""
        
        now = datetime.now()
        pub_time = article.published_date
        
        # Recency boost
        hours_old = (now - pub_time).total_seconds() / 3600
        if hours_old < 1:
            recency_score = 1.0
        elif hours_old < 4:
            recency_score = 0.7
        elif hours_old < 12:
            recency_score = 0.4
        else:
            recency_score = 0.1
        
        # Content-based urgency
        text = f"{article.title} {article.content}".lower()
        urgency_indicators = ['breaking', 'urgent', 'halt', 'suspend', 'emergency']
        urgency_boost = 0.3 if any(word in text for word in urgency_indicators) else 0.0
        
        return min(recency_score + urgency_boost, 1.0)
    
    def _find_keyword_matches(self, text: str) -> Dict[str, List[str]]:
        """Find and categorize keyword matches"""
        
        matches = {
            'high_impact': [kw for kw in self.high_impact_keywords if kw in text],
            'medium_impact': [kw for kw in self.medium_impact_keywords if kw in text]
        }
        
        return matches


class PilotNewsRouter:
    """
    Intelligent routing system for the LLM pilot that decides whether to use
    DeepSeek, Llama (future), or FinBERT based on content analysis and constraints.
    """
    
    def __init__(self, config: Optional[Dict[str, any]] = None):
        self.config = config or self._default_config()
        self.content_analyzer = NewsContentAnalyzer()
        
        # A/B testing configuration
        self.ab_test_enabled = self.config.get('ab_test_enabled', True)
        self.ab_test_ratio = self.config.get('ab_test_ratio', 0.2)  # 20% to LLM when eligible
        
        # Pilot symbol list (expand gradually)
        self.pilot_symbols = set(self.config.get('pilot_symbols', [
            # Week 1-2: Core tech stocks
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
            # Week 3: Add more tech
            'NVDA', 'META', 'NFLX', 'ADBE', 'CRM',
            # Week 4: Expand to other sectors
            'JPM', 'JNJ', 'PG', 'KO', 'DIS'
        ]))
        
        # Cost tracking for routing decisions
        self.daily_cost_limit = self.config.get('daily_cost_limit', 50.0)
        self.current_daily_cost = 0.0
        
        # Performance tracking
        self.routing_history = []
        
    def _default_config(self) -> Dict[str, any]:
        """Default routing configuration"""
        return {
            'ab_test_enabled': True,
            'ab_test_ratio': 0.2,
            'daily_cost_limit': 50.0,
            'complexity_threshold': 0.6,
            'importance_threshold': 0.5,
            'pilot_symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
            'cost_per_deepseek_request': 0.05  # Estimated average cost
        }
    
    def should_use_llm(self, article: NewsArticle) -> RoutingDecision:
        """
        Main routing decision function.
        
        Returns:
            RoutingDecision with processor choice and reasoning
        """
        
        # Check if article is eligible for pilot
        if not self._is_pilot_eligible(article):
            return RoutingDecision(
                processor='finbert',
                confidence=1.0,
                reasoning='Not in pilot symbol list',
                priority=3,
                expected_cost=0.0,
                expected_latency=0.1
            )
        
        # Check cost constraints
        if self._should_circuit_break_cost():
            return RoutingDecision(
                processor='finbert',
                confidence=1.0,
                reasoning='Daily cost limit reached',
                priority=3,
                expected_cost=0.0,
                expected_latency=0.1
            )
        
        # Analyze content complexity and importance
        analysis = self.content_analyzer.analyze_content(article)
        
        # Make routing decision based on analysis
        decision = self._make_routing_decision(article, analysis)
        
        # Record decision for monitoring
        self.routing_history.append({
            'timestamp': datetime.now(),
            'article_url': article.url,
            'symbols': article.symbols,
            'decision': decision,
            'analysis': analysis
        })
        
        return decision
    
    def _is_pilot_eligible(self, article: NewsArticle) -> bool:
        """Check if article is eligible for pilot (contains pilot symbols)"""
        
        if not article.symbols:
            return False
        
        # Check if any of the article's symbols are in the pilot list
        return any(symbol in self.pilot_symbols for symbol in article.symbols)
    
    def _should_circuit_break_cost(self) -> bool:
        """Check if daily cost limit would be exceeded"""
        
        estimated_request_cost = self.config['cost_per_deepseek_request']
        return (self.current_daily_cost + estimated_request_cost) > self.daily_cost_limit
    
    def _make_routing_decision(self, article: NewsArticle, analysis: Dict[str, any]) -> RoutingDecision:
        """Make the core routing decision based on content analysis"""
        
        complexity_score = analysis['complexity_score']
        importance_score = analysis['importance_score']
        time_sensitivity = analysis['time_sensitivity']
        
        # Decision matrix based on complexity and importance
        
        # High complexity OR high importance -> Consider DeepSeek
        if (complexity_score > self.config['complexity_threshold'] or 
            importance_score > self.config['importance_threshold']):
            
            # A/B testing logic for eligible articles
            if self.ab_test_enabled:
                should_use_deepseek = self._ab_test_decision(article)
                
                if should_use_deepseek:
                    return RoutingDecision(
                        processor='deepseek',
                        confidence=min(complexity_score + importance_score, 1.0),
                        reasoning=f'High complexity ({complexity_score:.2f}) or importance ({importance_score:.2f}), A/B test selected for DeepSeek',
                        priority=1 if time_sensitivity > 0.7 else 2,
                        expected_cost=self.config['cost_per_deepseek_request'],
                        expected_latency=2.5
                    )
                else:
                    return RoutingDecision(
                        processor='finbert',
                        confidence=0.8,
                        reasoning=f'Eligible for DeepSeek but A/B test selected FinBERT control group',
                        priority=2,
                        expected_cost=0.0,
                        expected_latency=0.1
                    )
            else:
                # No A/B testing, use DeepSeek for high complexity/importance
                return RoutingDecision(
                    processor='deepseek',
                    confidence=min(complexity_score + importance_score, 1.0),
                    reasoning=f'High complexity ({complexity_score:.2f}) or importance ({importance_score:.2f})',
                    priority=1 if time_sensitivity > 0.7 else 2,
                    expected_cost=self.config['cost_per_deepseek_request'],
                    expected_latency=2.5
                )
        
        # Low complexity and importance -> FinBERT
        else:
            return RoutingDecision(
                processor='finbert',
                confidence=0.9,
                reasoning=f'Low complexity ({complexity_score:.2f}) and importance ({importance_score:.2f})',
                priority=3,
                expected_cost=0.0,
                expected_latency=0.1
            )
    
    def _ab_test_decision(self, article: NewsArticle) -> bool:
        """
        Consistent A/B testing decision based on article characteristics.
        Same article will always get the same decision.
        """
        
        # Create consistent hash from article URL and date
        hash_input = f"{article.url}_{article.published_date.date()}"
        hash_value = hash(hash_input)
        
        # Use hash to make consistent decision
        return (hash_value % 100) < (self.ab_test_ratio * 100)
    
    def update_cost(self, cost: float):
        """Update daily cost tracking"""
        self.current_daily_cost += cost
    
    def reset_daily_cost(self):
        """Reset daily cost (should be called daily)"""
        self.current_daily_cost = 0.0
    
    def get_routing_stats(self, days_back: int = 1) -> Dict[str, any]:
        """Get routing statistics for monitoring"""
        
        # Filter to recent history
        cutoff_time = datetime.now() - timedelta(days=days_back)
        recent_history = [
            h for h in self.routing_history 
            if h['timestamp'] > cutoff_time
        ]
        
        if not recent_history:
            return {'message': 'No routing decisions in specified period'}
        
        # Calculate statistics
        total_decisions = len(recent_history)
        deepseek_decisions = sum(1 for h in recent_history if h['decision'].processor == 'deepseek')
        finbert_decisions = sum(1 for h in recent_history if h['decision'].processor == 'finbert')
        
        avg_complexity = sum(h['analysis']['complexity_score'] for h in recent_history) / total_decisions
        avg_importance = sum(h['analysis']['importance_score'] for h in recent_history) / total_decisions
        
        return {
            'period_days': days_back,
            'total_decisions': total_decisions,
            'deepseek_decisions': deepseek_decisions,
            'finbert_decisions': finbert_decisions,
            'deepseek_usage_rate': deepseek_decisions / total_decisions,
            'avg_complexity_score': avg_complexity,
            'avg_importance_score': avg_importance,
            'current_daily_cost': self.current_daily_cost,
            'cost_utilization': self.current_daily_cost / self.daily_cost_limit
        }
    
    def expand_pilot_symbols(self, new_symbols: List[str]):
        """Expand pilot to include new symbols"""
        
        self.pilot_symbols.update(new_symbols)
        logger.info(f"Pilot expanded to {len(self.pilot_symbols)} symbols: {sorted(self.pilot_symbols)}")
    
    def get_pilot_status(self) -> Dict[str, any]:
        """Get current pilot configuration status"""
        
        return {
            'pilot_symbols': sorted(list(self.pilot_symbols)),
            'symbol_count': len(self.pilot_symbols),
            'ab_test_enabled': self.ab_test_enabled,
            'ab_test_ratio': self.ab_test_ratio,
            'daily_cost_limit': self.daily_cost_limit,
            'current_daily_cost': self.current_daily_cost,
            'complexity_threshold': self.config['complexity_threshold'],
            'importance_threshold': self.config['importance_threshold']
        }


# Convenience function for external use
def route_news_article(article: NewsArticle, router: Optional[PilotNewsRouter] = None) -> RoutingDecision:
    """
    Convenience function to route a single news article.
    
    Args:
        article: NewsArticle to route
        router: Optional pre-configured router (creates default if None)
        
    Returns:
        RoutingDecision with processor choice and reasoning
    """
    
    if router is None:
        router = PilotNewsRouter()
    
    return router.should_use_llm(article)