"""
Monitoring, Metrics, and Cost Tracking for LLM Pilot

This module provides comprehensive monitoring capabilities for the DeepSeek pilot,
including cost tracking, performance monitoring, accuracy validation, and alerting.
"""

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any
from dataclasses import dataclass
from collections import deque, defaultdict
import asyncpg


logger = logging.getLogger(__name__)


@dataclass
class RequestMetrics:
    """Metrics for individual requests"""
    timestamp: datetime
    processor: str  # 'deepseek', 'finbert', 'error'
    latency: float
    cost: float
    input_tokens: int
    output_tokens: int
    success: bool
    article_url: str
    symbol: str


@dataclass
class DailyMetrics:
    """Aggregated daily metrics"""
    date: date
    total_requests: int
    deepseek_requests: int
    finbert_requests: int
    error_requests: int
    total_cost: float
    avg_latency: float
    success_rate: float
    accuracy_improvement: Optional[float]


class CostTracker:
    """
    Track and project LLM operational costs with budgeting and alerting.
    """

    def __init__(self, daily_budget: float = 50.0, monthly_budget: float = 1500.0):
        self.cost_per_1k_tokens = 0.014  # DeepSeek-R1 estimated pricing
        self.daily_budget = daily_budget
        self.monthly_budget = monthly_budget

        # Cost tracking
        self.daily_costs = defaultdict(float)  # date -> cost
        self.request_costs = []  # List of individual request costs

        # Budget alerts
        self.budget_alerts_sent = set()

    def calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for single request"""
        total_tokens = input_tokens + output_tokens
        cost = (total_tokens / 1000) * self.cost_per_1k_tokens

        # Record cost
        today = date.today()
        self.daily_costs[today] += cost
        self.request_costs.append({
            'timestamp': datetime.now(),
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'cost': cost
        })

        # Check budget alerts
        self._check_budget_alerts()

        return cost

    def get_daily_cost(self, target_date: Optional[date] = None) -> float:
        """Get cost for specific date (default: today)"""
        target_date = target_date or date.today()
        return self.daily_costs.get(target_date, 0.0)

    def get_weekly_cost(self) -> float:
        """Get cost for the last 7 days"""
        today = date.today()
        week_ago = today - timedelta(days=7)

        total = 0.0
        for i in range(7):
            check_date = week_ago + timedelta(days=i)
            total += self.daily_costs.get(check_date, 0.0)

        return total

    def get_monthly_cost(self) -> float:
        """Get cost for current month"""
        today = date.today()
        month_start = today.replace(day=1)

        total = 0.0
        current_date = month_start
        while current_date <= today:
            total += self.daily_costs.get(current_date, 0.0)
            current_date += timedelta(days=1)

        return total

    def project_monthly_cost(self) -> float:
        """Project monthly cost based on current daily average"""
        recent_costs = []
        today = date.today()

        # Get last 7 days of costs
        for i in range(7):
            check_date = today - timedelta(days=i)
            if check_date in self.daily_costs:
                recent_costs.append(self.daily_costs[check_date])

        if not recent_costs:
            return 0.0

        avg_daily_cost = sum(recent_costs) / len(recent_costs)
        days_in_month = 30  # Approximate

        return avg_daily_cost * days_in_month

    def get_cost_breakdown(self) -> Dict[str, Any]:
        """Detailed cost analysis"""
        total_requests = len(self.request_costs)

        if total_requests == 0:
            return {
                'total_requests': 0,
                'total_cost': 0.0,
                'avg_cost_per_request': 0.0,
                'daily_cost': 0.0,
                'weekly_cost': 0.0,
                'monthly_cost': 0.0,
                'projected_monthly': 0.0
            }

        total_cost = sum(r['cost'] for r in self.request_costs)

        return {
            'total_requests': total_requests,
            'total_cost': total_cost,
            'avg_cost_per_request': total_cost / total_requests,
            'daily_cost': self.get_daily_cost(),
            'weekly_cost': self.get_weekly_cost(),
            'monthly_cost': self.get_monthly_cost(),
            'projected_monthly': self.project_monthly_cost(),
            'budget_utilization': {
                'daily': self.get_daily_cost() / self.daily_budget,
                'monthly': self.get_monthly_cost() / self.monthly_budget
            }
        }

    def _check_budget_alerts(self):
        """Check if budget thresholds are exceeded and send alerts"""
        today = date.today()
        daily_cost = self.get_daily_cost(today)
        monthly_cost = self.get_monthly_cost()

        # Daily budget alert (80% threshold)
        if daily_cost > self.daily_budget * 0.8:
            alert_key = f"daily_{today}"
            if alert_key not in self.budget_alerts_sent:
                logger.warning(f"Daily budget alert: ${daily_cost:.2f} / ${self.daily_budget:.2f}")
                self.budget_alerts_sent.add(alert_key)

        # Monthly budget alert (80% threshold)
        if monthly_cost > self.monthly_budget * 0.8:
            alert_key = f"monthly_{today.strftime('%Y-%m')}"
            if alert_key not in self.budget_alerts_sent:
                logger.warning(f"Monthly budget alert: ${monthly_cost:.2f} / ${self.monthly_budget:.2f}")
                self.budget_alerts_sent.add(alert_key)

    def should_circuit_break(self) -> bool:
        """Check if cost circuit breaker should be triggered"""
        daily_cost = self.get_daily_cost()
        monthly_cost = self.get_monthly_cost()

        return (
            daily_cost > self.daily_budget or
            monthly_cost > self.monthly_budget
        )


class PerformanceMonitor:
    """
    Monitor performance metrics for the LLM pilot system.
    """

    def __init__(self, max_history: int = 10000):
        self.max_history = max_history
        self.request_history = deque(maxlen=max_history)

        # Aggregated metrics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_latency = 0.0

    def record_request(self, latency: float, response_size: int,
                      success: bool = True, processor: str = 'deepseek'):
        """Record metrics for a single request"""

        metrics = {
            'timestamp': datetime.now(),
            'latency': latency,
            'response_size': response_size,
            'success': success,
            'processor': processor
        }

        self.request_history.append(metrics)

        # Update aggregated metrics
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

        self.total_latency += latency

    def get_avg_latency(self, time_window: Optional[timedelta] = None) -> float:
        """Get average latency, optionally within time window"""

        if not self.request_history:
            return 0.0

        if time_window:
            cutoff_time = datetime.now() - time_window
            recent_requests = [
                r for r in self.request_history
                if r['timestamp'] > cutoff_time
            ]
        else:
            recent_requests = list(self.request_history)

        if not recent_requests:
            return 0.0

        total_latency = sum(r['latency'] for r in recent_requests)
        return total_latency / len(recent_requests)

    def get_success_rate(self, time_window: Optional[timedelta] = None) -> float:
        """Get success rate, optionally within time window"""

        if not self.request_history:
            return 0.0

        if time_window:
            cutoff_time = datetime.now() - time_window
            recent_requests = [
                r for r in self.request_history
                if r['timestamp'] > cutoff_time
            ]
        else:
            recent_requests = list(self.request_history)

        if not recent_requests:
            return 0.0

        successful = sum(1 for r in recent_requests if r['success'])
        return successful / len(recent_requests)

    def get_throughput(self, time_window: timedelta = timedelta(hours=1)) -> float:
        """Get requests per second over time window"""

        cutoff_time = datetime.now() - time_window
        recent_requests = [
            r for r in self.request_history
            if r['timestamp'] > cutoff_time
        ]

        if not recent_requests:
            return 0.0

        return len(recent_requests) / time_window.total_seconds()

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""

        return {
            'total_requests': self.total_requests,
            'success_rate': self.get_success_rate(),
            'avg_latency': self.get_avg_latency(),
            'recent_latency': self.get_avg_latency(timedelta(hours=1)),
            'hourly_throughput': self.get_throughput(timedelta(hours=1)),
            'daily_throughput': self.get_throughput(timedelta(days=1)),
            'error_rate': 1.0 - self.get_success_rate() if self.total_requests > 0 else 0.0
        }


class AccuracyValidator:
    """
    Validate LLM accuracy against ground truth and FinBERT baseline.
    """

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.accuracy_samples = []

    async def validate_accuracy(self, article_url: str, symbol: str,
                              deepseek_sentiment: float, finbert_sentiment: float) -> Dict[str, float]:
        """
        Validate accuracy by measuring actual price reaction.

        This is a proxy for accuracy - we measure how well the sentiment
        predicts actual stock price movement over the next 24 hours.
        """

        try:
            # Get price reaction (24-hour forward return)
            price_reaction = await self._get_price_reaction(symbol)

            if price_reaction is None:
                return {'error': 'Unable to get price data'}

            # Calculate directional accuracy
            deepseek_accuracy = self._calculate_directional_accuracy(
                deepseek_sentiment, price_reaction
            )

            finbert_accuracy = self._calculate_directional_accuracy(
                finbert_sentiment, price_reaction
            )

            # Store accuracy sample
            sample = {
                'timestamp': datetime.now(),
                'article_url': article_url,
                'symbol': symbol,
                'deepseek_sentiment': deepseek_sentiment,
                'finbert_sentiment': finbert_sentiment,
                'price_reaction': price_reaction,
                'deepseek_accuracy': deepseek_accuracy,
                'finbert_accuracy': finbert_accuracy,
                'improvement': deepseek_accuracy - finbert_accuracy
            }

            self.accuracy_samples.append(sample)

            # Store in database for persistence
            await self._store_accuracy_sample(sample)

            return {
                'deepseek_accuracy': deepseek_accuracy,
                'finbert_accuracy': finbert_accuracy,
                'improvement': deepseek_accuracy - finbert_accuracy,
                'price_reaction': price_reaction
            }

        except Exception as e:
            logger.error(f"Accuracy validation failed for {symbol}: {e}")
            return {'error': str(e)}

    async def _get_price_reaction(self, symbol: str) -> Optional[float]:
        """Get 24-hour forward price reaction for the symbol"""

        try:
            async with self.pool.acquire() as conn:
                # Get the most recent price and price 24 hours later
                # This is a simplified version - in practice you'd want to be more precise about timing
                result = await conn.fetchrow("""
                    SELECT
                        close_price as current_price,
                        LAG(close_price, 1) OVER (ORDER BY timestamp DESC) as previous_price
                    FROM dev_daily_price_polygon
                    WHERE symbol = $1
                        AND timestamp >= NOW() - INTERVAL '2 days'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, symbol)

                if not result or not result['previous_price']:
                    return None

                # Calculate percentage change
                current = float(result['current_price'])
                previous = float(result['previous_price'])

                return (current - previous) / previous

        except Exception as e:
            logger.error(f"Failed to get price reaction for {symbol}: {e}")
            return None

    def _calculate_directional_accuracy(self, sentiment: float, price_reaction: float) -> float:
        """
        Calculate directional accuracy - how well sentiment predicts price direction.

        Returns 1.0 for correct direction, 0.0 for incorrect direction,
        with partial credit based on magnitude.
        """

        # Determine predicted and actual directions
        predicted_positive = sentiment > 0.1
        predicted_negative = sentiment < -0.1
        actual_positive = price_reaction > 0.005  # 0.5% threshold
        actual_negative = price_reaction < -0.005

        # Perfect directional match
        if (predicted_positive and actual_positive) or (predicted_negative and actual_negative):
            return 1.0

        # Wrong direction
        if (predicted_positive and actual_negative) or (predicted_negative and actual_positive):
            return 0.0

        # Neutral predictions or small price movements
        # Give partial credit based on magnitude alignment
        sentiment_magnitude = abs(sentiment)
        price_magnitude = abs(price_reaction)

        # If both are small (neutral), that's reasonably accurate
        if sentiment_magnitude < 0.1 and price_magnitude < 0.005:
            return 0.7

        # Otherwise, partial credit based on how close they are
        return 0.5

    async def _store_accuracy_sample(self, sample: Dict[str, Any]):
        """Store accuracy sample in database for persistence"""

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO llm_pilot_accuracy_samples
                    (timestamp, article_url, symbol, deepseek_sentiment, finbert_sentiment,
                     price_reaction, deepseek_accuracy, finbert_accuracy, improvement)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                sample['timestamp'], sample['article_url'], sample['symbol'],
                sample['deepseek_sentiment'], sample['finbert_sentiment'],
                sample['price_reaction'], sample['deepseek_accuracy'],
                sample['finbert_accuracy'], sample['improvement'])

        except Exception as e:
            logger.error(f"Failed to store accuracy sample: {e}")

    def get_accuracy_summary(self, days_back: int = 7) -> Dict[str, Any]:
        """Get accuracy summary for the last N days"""

        cutoff_time = datetime.now() - timedelta(days=days_back)
        recent_samples = [
            s for s in self.accuracy_samples
            if s['timestamp'] > cutoff_time
        ]

        if not recent_samples:
            return {
                'sample_count': 0,
                'deepseek_avg_accuracy': 0.0,
                'finbert_avg_accuracy': 0.0,
                'avg_improvement': 0.0
            }

        deepseek_accuracies = [s['deepseek_accuracy'] for s in recent_samples]
        finbert_accuracies = [s['finbert_accuracy'] for s in recent_samples]
        improvements = [s['improvement'] for s in recent_samples]

        return {
            'sample_count': len(recent_samples),
            'deepseek_avg_accuracy': sum(deepseek_accuracies) / len(deepseek_accuracies),
            'finbert_avg_accuracy': sum(finbert_accuracies) / len(finbert_accuracies),
            'avg_improvement': sum(improvements) / len(improvements),
            'improvement_percentage': (sum(improvements) / len(improvements)) /
                                   (sum(finbert_accuracies) / len(finbert_accuracies)) * 100
        }


class PilotMonitor:
    """
    Main monitoring system that combines cost tracking, performance monitoring,
    and accuracy validation.
    """

    def __init__(self, pool: asyncpg.Pool, daily_budget: float = 50.0):
        self.pool = pool

        # Initialize component monitors
        self.cost_tracker = CostTracker(daily_budget=daily_budget)
        self.performance_monitor = PerformanceMonitor()
        self.accuracy_validator = AccuracyValidator(pool)

        # Request history for detailed analysis
        self.request_history = []

    async def record_request(self, article_url: str, symbol: str, processor: str,
                           latency: float, cost: float, input_tokens: int, output_tokens: int,
                           success: bool, deepseek_sentiment: Optional[float] = None,
                           finbert_sentiment: Optional[float] = None):
        """Record comprehensive request metrics"""

        # Create request metrics object
        metrics = RequestMetrics(
            timestamp=datetime.now(),
            processor=processor,
            latency=latency,
            cost=cost,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            success=success,
            article_url=article_url,
            symbol=symbol
        )

        self.request_history.append(metrics)

        # Update component monitors
        self.performance_monitor.record_request(
            latency, output_tokens, success, processor
        )

        # Validate accuracy if we have both sentiment scores
        if deepseek_sentiment is not None and finbert_sentiment is not None:
            accuracy_result = await self.accuracy_validator.validate_accuracy(
                article_url, symbol, deepseek_sentiment, finbert_sentiment
            )
            metrics.accuracy_result = accuracy_result

    def get_daily_report(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """Generate comprehensive daily report"""

        target_date = target_date or date.today()

        # Filter requests for target date
        daily_requests = [
            r for r in self.request_history
            if r.timestamp.date() == target_date
        ]

        if not daily_requests:
            return {
                'date': target_date,
                'total_requests': 0,
                'message': 'No requests processed on this date'
            }

        # Calculate daily metrics
        total_requests = len(daily_requests)
        deepseek_requests = len([r for r in daily_requests if r.processor == 'deepseek'])
        finbert_requests = len([r for r in daily_requests if r.processor == 'finbert'])
        error_requests = len([r for r in daily_requests if not r.success])

        total_cost = sum(r.cost for r in daily_requests)
        avg_latency = sum(r.latency for r in daily_requests) / total_requests
        success_rate = (total_requests - error_requests) / total_requests

        # Get accuracy summary
        accuracy_summary = self.accuracy_validator.get_accuracy_summary(days_back=1)

        report = {
            'date': target_date,
            'requests': {
                'total': total_requests,
                'deepseek': deepseek_requests,
                'finbert': finbert_requests,
                'errors': error_requests,
                'deepseek_usage_rate': deepseek_requests / total_requests if total_requests > 0 else 0
            },
            'performance': {
                'avg_latency': avg_latency,
                'success_rate': success_rate,
                'error_rate': error_requests / total_requests if total_requests > 0 else 0
            },
            'costs': {
                'daily_cost': total_cost,
                'avg_cost_per_request': total_cost / total_requests if total_requests > 0 else 0,
                'projected_monthly': self.cost_tracker.project_monthly_cost()
            },
            'accuracy': accuracy_summary
        }

        return report

    def get_pilot_summary(self) -> Dict[str, Any]:
        """Get comprehensive pilot performance summary"""

        # Overall metrics
        total_requests = len(self.request_history)
        if total_requests == 0:
            return {'message': 'No requests processed yet'}

        deepseek_requests = len([r for r in self.request_history if r.processor == 'deepseek'])
        finbert_requests = len([r for r in self.request_history if r.processor == 'finbert'])

        # Performance metrics
        performance = self.performance_monitor.get_performance_summary()

        # Cost metrics
        cost_breakdown = self.cost_tracker.get_cost_breakdown()

        # Accuracy metrics
        accuracy_summary = self.accuracy_validator.get_accuracy_summary(days_back=7)

        return {
            'pilot_duration': (datetime.now() - min(r.timestamp for r in self.request_history)).days,
            'requests': {
                'total': total_requests,
                'deepseek': deepseek_requests,
                'finbert': finbert_requests,
                'deepseek_usage_rate': deepseek_requests / total_requests
            },
            'performance': performance,
            'costs': cost_breakdown,
            'accuracy': accuracy_summary,
            'go_no_go_criteria': self._evaluate_go_no_go_criteria(
                performance, cost_breakdown, accuracy_summary
            )
        }

    def _evaluate_go_no_go_criteria(self, performance: Dict, costs: Dict,
                                   accuracy: Dict) -> Dict[str, Any]:
        """Evaluate against go/no-go decision criteria"""

        criteria = {
            'accuracy_improvement': {
                'target': 10.0,  # >10% improvement
                'actual': accuracy.get('improvement_percentage', 0.0),
                'met': accuracy.get('improvement_percentage', 0.0) > 10.0
            },
            'monthly_cost_projection': {
                'target': 2000.0,  # <$2000/month
                'actual': costs.get('projected_monthly', 0.0),
                'met': costs.get('projected_monthly', 0.0) < 2000.0
            },
            'system_reliability': {
                'target': 99.5,  # >99.5% success rate
                'actual': performance.get('success_rate', 0.0) * 100,
                'met': performance.get('success_rate', 0.0) > 0.995
            },
            'processing_latency': {
                'target': 3.0,  # <3s average
                'actual': performance.get('avg_latency', 0.0),
                'met': performance.get('avg_latency', 0.0) < 3.0
            }
        }

        # Calculate overall score
        met_criteria = sum(1 for c in criteria.values() if c['met'])
        total_criteria = len(criteria)
        overall_score = met_criteria / total_criteria

        # Determine recommendation
        if overall_score >= 0.75:  # 3/4 criteria met
            recommendation = "GO - Proceed with full implementation"
        elif overall_score >= 0.5:  # 2/4 criteria met
            recommendation = "CONDITIONAL GO - Address specific issues"
        else:
            recommendation = "NO GO - Revisit approach"

        return {
            'criteria': criteria,
            'overall_score': overall_score,
            'recommendation': recommendation,
            'summary': f"Met {met_criteria}/{total_criteria} criteria ({overall_score:.1%})"
        }


# Database schema for accuracy samples
CREATE_ACCURACY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS llm_pilot_accuracy_samples (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    article_url VARCHAR(500) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    deepseek_sentiment FLOAT NOT NULL,
    finbert_sentiment FLOAT NOT NULL,
    price_reaction FLOAT NOT NULL,
    deepseek_accuracy FLOAT NOT NULL,
    finbert_accuracy FLOAT NOT NULL,
    improvement FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_accuracy_samples_timestamp ON llm_pilot_accuracy_samples(timestamp);
CREATE INDEX IF NOT EXISTS idx_accuracy_samples_symbol ON llm_pilot_accuracy_samples(symbol);
"""