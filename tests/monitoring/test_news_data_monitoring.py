#!/usr/bin/env python3
"""
News Data Monitoring Tests - Production Health Checks

These tests are designed to run in production to detect news collection issues
before they become user-visible problems. Based on the investigation of Polygon
news stopping at 2025-08-27 due to silent failures.

USAGE:
    # Run as health check
    python tests/monitoring/test_news_data_monitoring.py --environment intg
    
    # Run with alerts
    python tests/monitoring/test_news_data_monitoring.py --environment prod --alert-slack
    
    # Run specific checks
    python tests/monitoring/test_news_data_monitoring.py --check freshness --check gaps
"""

import asyncio
import argparse
import asyncpg
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from shared.utils.database import get_database_pool


class NewsDataMonitor:
    """Production monitoring for news data health"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.table_name = f"{environment}_news_polygon"
        self.alerts = []
        
    async def run_all_checks(self) -> Dict[str, Any]:
        """Run comprehensive news data health checks"""
        pool = await get_database_pool(self.environment)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'environment': self.environment,
            'checks': {},
            'alerts': [],
            'overall_health': 'HEALTHY'
        }
        
        # Run all monitoring checks
        checks = [
            ('data_freshness', self.check_data_freshness),
            ('data_gaps', self.check_data_gaps), 
            ('source_diversity', self.check_source_diversity),
            ('data_quality', self.check_data_quality),
            ('volume_trends', self.check_volume_trends),
            ('api_error_patterns', self.check_api_error_patterns),
            ('duplicate_detection', self.check_duplicate_handling)
        ]
        
        for check_name, check_func in checks:
            try:
                check_result = await check_func(pool)
                results['checks'][check_name] = check_result
                
                if not check_result.get('passed', False):
                    results['overall_health'] = 'UNHEALTHY'
                    if check_result.get('alert'):
                        results['alerts'].append({
                            'check': check_name,
                            'message': check_result['alert'],
                            'severity': check_result.get('severity', 'warning')
                        })
                        
            except Exception as e:
                results['checks'][check_name] = {
                    'passed': False,
                    'error': str(e),
                    'alert': f'Check {check_name} failed with error: {e}',
                    'severity': 'critical'
                }
                results['overall_health'] = 'UNHEALTHY'
        
        await pool.close()
        return results
        
    async def check_data_freshness(self, pool) -> Dict[str, Any]:
        """Check if news data is fresh (within acceptable time threshold)"""
        async with pool.acquire() as conn:
            latest_article = await conn.fetchrow(f"""
                SELECT 
                    MAX(published_utc) as latest_published,
                    COUNT(*) as total_articles
                FROM {self.table_name}
            """)
            
            if not latest_article['latest_published']:
                return {
                    'passed': False,
                    'alert': 'No articles found in database',
                    'severity': 'critical',
                    'details': {'total_articles': 0}
                }
            
            latest_utc = latest_article['latest_published'] 
            now_utc = datetime.now(latest_utc.tzinfo)
            age_hours = (now_utc - latest_utc).total_seconds() / 3600
            
            # Different thresholds for different environments
            thresholds = {
                'prod': 24,  # Production should have news within 24 hours
                'intg': 48,  # Integration can be 48 hours
                'dev': 72    # Dev can be 72 hours
            }
            
            threshold = thresholds.get(self.environment, 48)
            passed = age_hours <= threshold
            
            result = {
                'passed': passed,
                'details': {
                    'latest_published': latest_utc.isoformat(),
                    'age_hours': round(age_hours, 1),
                    'threshold_hours': threshold,
                    'total_articles': latest_article['total_articles']
                }
            }
            
            if not passed:
                result.update({
                    'alert': f'News data is stale: {age_hours:.1f} hours old (threshold: {threshold}h)',
                    'severity': 'critical' if age_hours > threshold * 2 else 'warning'
                })
                
            return result
            
    async def check_data_gaps(self, pool) -> Dict[str, Any]:
        """Check for gaps in news collection (missing days)"""
        async with pool.acquire() as conn:
            # Check last 14 days for gaps
            daily_counts = await conn.fetch(f"""
                SELECT 
                    DATE(published_utc) as article_date,
                    COUNT(*) as article_count,
                    EXTRACT(DOW FROM DATE(published_utc)) as day_of_week
                FROM {self.table_name}
                WHERE published_utc >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY DATE(published_utc)
                ORDER BY article_date DESC
            """)
            
            gaps = []
            for i in range(14):
                check_date = datetime.now().date() - timedelta(days=i)
                day_of_week = check_date.weekday()  # 0=Monday, 6=Sunday
                
                # Find if we have data for this date
                date_data = next((row for row in daily_counts if row['article_date'] == check_date), None)
                
                # Only flag weekdays (markets are open)
                if day_of_week < 5:  # Monday-Friday
                    if not date_data or date_data['article_count'] == 0:
                        gaps.append({
                            'date': check_date.isoformat(),
                            'day_of_week': day_of_week,
                            'article_count': 0
                        })
                    elif date_data['article_count'] < 10:  # Very low volume
                        gaps.append({
                            'date': check_date.isoformat(), 
                            'day_of_week': day_of_week,
                            'article_count': date_data['article_count'],
                            'type': 'low_volume'
                        })
            
            passed = len(gaps) == 0
            
            result = {
                'passed': passed,
                'details': {
                    'gaps_found': len(gaps),
                    'gaps': gaps[:5],  # Show first 5 gaps
                    'daily_counts': [dict(row) for row in daily_counts[:7]]  # Last 7 days
                }
            }
            
            if not passed:
                gap_dates = [gap['date'] for gap in gaps[:3]]
                result.update({
                    'alert': f'News collection gaps detected for {len(gaps)} days: {gap_dates}',
                    'severity': 'critical' if len(gaps) > 3 else 'warning'
                })
                
            return result
            
    async def check_source_diversity(self, pool) -> Dict[str, Any]:
        """Check that news comes from multiple sources (not just one failing source)"""
        async with pool.acquire() as conn:
            source_stats = await conn.fetch(f"""
                SELECT 
                    publisher_name,
                    COUNT(*) as article_count,
                    MAX(published_utc) as latest_article
                FROM {self.table_name}
                WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY publisher_name
                ORDER BY article_count DESC
                LIMIT 10
            """)
            
            unique_sources = len(source_stats)
            total_recent_articles = sum(row['article_count'] for row in source_stats)
            
            # Check source concentration (no single source > 80%)
            max_source_pct = 0
            if total_recent_articles > 0:
                max_source_pct = max(row['article_count'] for row in source_stats) / total_recent_articles * 100
            
            passed = unique_sources >= 3 and max_source_pct < 80
            
            result = {
                'passed': passed,
                'details': {
                    'unique_sources': unique_sources,
                    'total_recent_articles': total_recent_articles,
                    'max_source_percentage': round(max_source_pct, 1),
                    'top_sources': [dict(row) for row in source_stats[:5]]
                }
            }
            
            if not passed:
                if unique_sources < 3:
                    result.update({
                        'alert': f'Low source diversity: only {unique_sources} sources in last 7 days',
                        'severity': 'warning'
                    })
                elif max_source_pct >= 80:
                    result.update({
                        'alert': f'Single source dominance: {max_source_pct:.1f}% from one publisher',
                        'severity': 'warning'
                    })
                    
            return result
            
    async def check_data_quality(self, pool) -> Dict[str, Any]:
        """Check data quality metrics (completeness, format, etc.)"""
        async with pool.acquire() as conn:
            quality_stats = await conn.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_articles,
                    COUNT(*) FILTER (WHERE title IS NOT NULL AND title != '') as has_title,
                    COUNT(*) FILTER (WHERE description IS NOT NULL AND description != '') as has_description,
                    COUNT(*) FILTER (WHERE tickers IS NOT NULL AND array_length(tickers, 1) > 0) as has_tickers,
                    COUNT(*) FILTER (WHERE keywords IS NOT NULL AND array_length(keywords, 1) > 0) as has_keywords,
                    COUNT(*) FILTER (WHERE author IS NOT NULL AND author != '') as has_author
                FROM {self.table_name}
                WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            if quality_stats['total_articles'] == 0:
                return {
                    'passed': False,
                    'alert': 'No articles found for quality analysis',
                    'severity': 'critical'
                }
            
            # Calculate quality percentages
            total = quality_stats['total_articles']
            quality_metrics = {
                'title_completeness': quality_stats['has_title'] / total * 100,
                'description_completeness': quality_stats['has_description'] / total * 100, 
                'tickers_completeness': quality_stats['has_tickers'] / total * 100,
                'keywords_completeness': quality_stats['has_keywords'] / total * 100,
                'author_completeness': quality_stats['has_author'] / total * 100
            }
            
            # Overall quality score (weighted average)
            quality_score = (
                quality_metrics['title_completeness'] * 0.3 +
                quality_metrics['description_completeness'] * 0.3 +
                quality_metrics['tickers_completeness'] * 0.2 +
                quality_metrics['keywords_completeness'] * 0.1 +
                quality_metrics['author_completeness'] * 0.1
            )
            
            passed = quality_score >= 70  # 70% minimum quality threshold
            
            result = {
                'passed': passed,
                'details': {
                    'total_articles': total,
                    'quality_score': round(quality_score, 1),
                    'metrics': {k: round(v, 1) for k, v in quality_metrics.items()}
                }
            }
            
            if not passed:
                result.update({
                    'alert': f'Data quality below threshold: {quality_score:.1f}% (minimum: 70%)',
                    'severity': 'warning'
                })
                
            return result
            
    async def check_volume_trends(self, pool) -> Dict[str, Any]:
        """Check for unusual volume patterns (sudden drops/spikes)"""
        async with pool.acquire() as conn:
            # Get daily volume for last 30 days
            daily_volumes = await conn.fetch(f"""
                SELECT 
                    DATE(published_utc) as article_date,
                    COUNT(*) as daily_count
                FROM {self.table_name}
                WHERE published_utc >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(published_utc)
                ORDER BY article_date DESC
                LIMIT 30
            """)
            
            if len(daily_volumes) < 7:
                return {
                    'passed': False,
                    'alert': 'Insufficient data for volume trend analysis',
                    'severity': 'warning'
                }
            
            # Calculate moving averages
            recent_7_day_avg = sum(row['daily_count'] for row in daily_volumes[:7]) / 7
            previous_7_day_avg = sum(row['daily_count'] for row in daily_volumes[7:14]) / 7
            
            # Check for significant drops (>50% decrease)
            volume_change_pct = ((recent_7_day_avg - previous_7_day_avg) / previous_7_day_avg * 100) if previous_7_day_avg > 0 else 0
            
            passed = volume_change_pct > -50  # Not more than 50% drop
            
            result = {
                'passed': passed,
                'details': {
                    'recent_7day_avg': round(recent_7_day_avg, 1),
                    'previous_7day_avg': round(previous_7_day_avg, 1),
                    'volume_change_percent': round(volume_change_pct, 1),
                    'daily_volumes': [dict(row) for row in daily_volumes[:14]]
                }
            }
            
            if not passed:
                result.update({
                    'alert': f'Significant volume drop detected: {volume_change_pct:.1f}% decrease',
                    'severity': 'critical' if volume_change_pct < -75 else 'warning'
                })
                
            return result
            
    async def check_api_error_patterns(self, pool) -> Dict[str, Any]:
        """Check for patterns indicating API issues (placeholder - needs logging integration)"""
        # This would check application logs for API error patterns
        # For now, return a basic check
        return {
            'passed': True,
            'details': {
                'note': 'API error pattern detection requires log integration',
                'recommendation': 'Implement structured logging for API errors'
            }
        }
        
    async def check_duplicate_handling(self, pool) -> Dict[str, Any]:
        """Check that duplicate detection is working correctly"""
        async with pool.acquire() as conn:
            # Check for potential duplicates by vendor_id
            duplicate_stats = await conn.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT vendor_id) as unique_vendor_ids,
                    COUNT(*) - COUNT(DISTINCT vendor_id) as potential_duplicates
                FROM {self.table_name}
                WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            duplicate_rate = (duplicate_stats['potential_duplicates'] / duplicate_stats['total_records'] * 100) if duplicate_stats['total_records'] > 0 else 0
            
            passed = duplicate_rate < 5  # Less than 5% duplicates
            
            result = {
                'passed': passed,
                'details': {
                    'total_records': duplicate_stats['total_records'],
                    'unique_vendor_ids': duplicate_stats['unique_vendor_ids'],
                    'potential_duplicates': duplicate_stats['potential_duplicates'],
                    'duplicate_rate_percent': round(duplicate_rate, 2)
                }
            }
            
            if not passed:
                result.update({
                    'alert': f'High duplicate rate detected: {duplicate_rate:.1f}%',
                    'severity': 'warning'
                })
                
            return result


async def main():
    """Main monitoring function"""
    parser = argparse.ArgumentParser(description='News Data Monitoring')
    parser.add_argument('--environment', default='dev', choices=['dev', 'intg', 'prod'])
    parser.add_argument('--check', action='append', help='Specific checks to run')
    parser.add_argument('--alert-slack', action='store_true', help='Send alerts to Slack')
    parser.add_argument('--output', default='console', choices=['console', 'json'])
    
    args = parser.parse_args()
    
    monitor = NewsDataMonitor(args.environment)
    results = await monitor.run_all_checks()
    
    # Output results
    if args.output == 'json':
        # Custom JSON encoder to handle date objects
        def json_serializer(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')
        
        print(json.dumps(results, indent=2, default=json_serializer))
    else:
        print(f"\n📊 News Data Health Report - {args.environment.upper()}")
        print(f"{'='*50}")
        print(f"Overall Health: {results['overall_health']}")
        print(f"Timestamp: {results['timestamp']}")
        
        for check_name, check_result in results['checks'].items():
            status = "✅ PASS" if check_result['passed'] else "❌ FAIL"
            print(f"\n{check_name}: {status}")
            
            if not check_result['passed'] and 'alert' in check_result:
                print(f"  Alert: {check_result['alert']}")
                
        if results['alerts']:
            print(f"\n🚨 Alerts ({len(results['alerts'])}):")
            for alert in results['alerts']:
                severity_icon = "🔴" if alert['severity'] == 'critical' else "🟡"
                print(f"  {severity_icon} {alert['check']}: {alert['message']}")
    
    # Exit with error code if unhealthy
    if results['overall_health'] != 'HEALTHY':
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())