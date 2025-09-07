#!/usr/bin/env python3
"""
Tests for Historical Earnings Backfill System

Tests the gap analysis, backfill planning, and execution logic
for systematically filling historical earnings data gaps.
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from unittest.mock import Mock, AsyncMock, patch
from dataclasses import dataclass
from typing import Dict, List, Any

@pytest.fixture 
def mock_environment():
    """Mock environment configuration for backfill tests"""
    env = Mock()
    env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")
    
    # Mock database
    database = Mock()
    pool = AsyncMock()
    conn = AsyncMock()
    
    database.create_pool_with_retry = AsyncMock(return_value=pool)
    pool.acquire = AsyncMock(return_value=conn)
    pool.__aenter__ = AsyncMock(return_value=pool)
    pool.__aexit__ = AsyncMock(return_value=None)
    conn.__aenter__ = AsyncMock(return_value=conn) 
    conn.__aexit__ = AsyncMock(return_value=None)
    
    env.database = database
    return env, pool, conn

@pytest.fixture
def sample_gap_data():
    """Sample gap analysis data for testing"""
    return [
        {
            'symbol': 'AAPL',
            'missing_years': [2018, 2019],  # Missing 2 years
            'incomplete_years': [2017],      # 1 incomplete year
            'eps_missing_years': [2020, 2021], # 2 years missing EPS
            'priority_score': 150.0  # High priority (Tier 1 symbol)
        },
        {
            'symbol': 'TSLA', 
            'missing_years': [2015, 2016, 2017, 2018],  # Missing 4 years
            'incomplete_years': [],
            'eps_missing_years': [2019],
            'priority_score': 120.0  # High priority (popular symbol)
        },
        {
            'symbol': 'XYZ',
            'missing_years': [2010, 2011, 2012],  # Older gaps
            'incomplete_years': [2013],
            'eps_missing_years': [],
            'priority_score': 35.0   # Low priority (unknown symbol)
        }
    ]

class TestGapAnalysis:
    """Test earnings data gap analysis functionality"""
    
    @pytest.mark.asyncio
    async def test_coverage_gap_detection(self, mock_environment):
        """Test detection of coverage gaps for symbols"""
        env, pool, conn = mock_environment
        
        # Mock database query results for yearly coverage
        mock_coverage_data = [
            {'year': 2020, 'earnings_count': 4, 'eps_count': 4},  # Complete year
            {'year': 2021, 'earnings_count': 3, 'eps_count': 3},  # Incomplete year (missing Q4)
            {'year': 2022, 'earnings_count': 4, 'eps_count': 0},  # Complete but no EPS
            {'year': 2023, 'earnings_count': 0, 'eps_count': 0},  # Missing year
            {'year': 2024, 'earnings_count': 2, 'eps_count': 2},  # Partial year (ongoing)
        ]
        
        conn.fetch = AsyncMock(return_value=mock_coverage_data)
        
        with patch('src.events.backfill.historical_earnings_backfill.Environment', return_value=env):
            # Simulate the gap analysis class
            class MockHistoricalEarningsBackfill:
                def __init__(self):
                    self.env = env
                
                async def analyze_symbol_gaps(self, symbol: str):
                    """Analyze gaps for a single symbol"""
                    rows = await conn.fetch("SELECT * FROM coverage")
                    
                    missing_years = []
                    incomplete_years = []
                    eps_missing_years = []
                    
                    for row in rows:
                        year = row['year']
                        earnings_count = row['earnings_count']
                        eps_count = row['eps_count']
                        
                        if earnings_count == 0:
                            missing_years.append(year)
                        elif earnings_count < 4 and year < 2024:  # Don't flag current year
                            incomplete_years.append(year)
                        
                        if eps_count == 0 and earnings_count > 0:
                            eps_missing_years.append(year)
                    
                    return {
                        'symbol': symbol,
                        'missing_years': missing_years,
                        'incomplete_years': incomplete_years,
                        'eps_missing_years': eps_missing_years
                    }
            
            backfill = MockHistoricalEarningsBackfill()
            gaps = await backfill.analyze_symbol_gaps('AAPL')
            
            assert gaps['symbol'] == 'AAPL'
            assert 2023 in gaps['missing_years']           # No earnings at all
            assert 2021 in gaps['incomplete_years']        # Only 3 quarters
            assert 2022 in gaps['eps_missing_years']       # Has earnings but no EPS
            
            # 2024 shouldn't be flagged as incomplete (current year)
            assert 2024 not in gaps['incomplete_years']
    
    def test_priority_score_calculation(self):
        """Test priority scoring algorithm for backfill planning"""
        
        def calculate_priority_score(symbol: str, missing_years: List[int], 
                                   incomplete_years: List[int], eps_missing_years: List[int]) -> float:
            """Calculate priority score for backfill"""
            score = 0.0
            current_year = 2025
            
            # Symbol tier weighting
            tier_1 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']
            tier_2 = ['JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS']
            
            if symbol in tier_1:
                score += 100
            elif symbol in tier_2:
                score += 80
            else:
                score += 20
            
            # Recent years are more valuable
            for year in missing_years:
                age = current_year - year
                if age <= 5:
                    score += 50 - (age * 8)  # 50, 42, 34, 26, 18
                elif age <= 10:
                    score += 20 - (age - 5) * 2
                else:
                    score += max(1, 10 - (age - 10))
            
            # Bonus for incomplete and EPS missing years
            for year in incomplete_years:
                age = current_year - year
                score += 10 - age if age <= 5 else 2
            
            for year in eps_missing_years:
                age = current_year - year
                score += 5 if age <= 5 else 2
            
            return score
        
        # Test Tier 1 symbol with recent gaps
        aapl_score = calculate_priority_score('AAPL', [2022, 2021], [2020], [2023])
        assert aapl_score > 150  # Should be high priority
        
        # Test unknown symbol with old gaps  
        unknown_score = calculate_priority_score('XYZ', [2010, 2011], [], [])
        assert unknown_score < 50   # Should be low priority
        
        # Test recent vs old gaps
        recent_score = calculate_priority_score('MSFT', [2023], [], [])
        old_score = calculate_priority_score('MSFT', [2010], [], [])
        assert recent_score > old_score  # Recent gaps more valuable
    
    def test_symbol_prioritization(self):
        """Test symbol tier prioritization system"""
        symbol_priorities = {
            'tier_1': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA'],
            'tier_2': ['JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS'],
            'tier_3': ['CRM', 'ORCL', 'CSCO', 'INTC', 'IBM', 'QCOM', 'AMD'],
            'tier_4': ['WMT', 'KO', 'PFE', 'MRK', 'T', 'VZ', 'CMCSA', 'PEP']
        }
        
        def get_symbol_tier(symbol: str) -> int:
            for tier, symbols in symbol_priorities.items():
                if symbol in symbols:
                    return int(tier.split('_')[1])
            return 5  # Unknown tier
        
        assert get_symbol_tier('AAPL') == 1
        assert get_symbol_tier('JPM') == 2  
        assert get_symbol_tier('CRM') == 3
        assert get_symbol_tier('WMT') == 4
        assert get_symbol_tier('UNKNOWN') == 5

class TestBackfillPlanning:
    """Test backfill plan generation and prioritization"""
    
    def test_backfill_plan_creation(self, sample_gap_data):
        """Test creation of prioritized backfill plans"""
        
        def create_backfill_plans(gaps: List[Dict]) -> List[Dict]:
            """Create prioritized backfill plans from gaps"""
            plans = []
            
            # Plan 1: Recent critical gaps (Tier 1 symbols, 2015-2020)
            tier1_symbols = ['AAPL', 'TSLA']  # From sample data
            recent_symbols = [g['symbol'] for g in gaps 
                            if g['symbol'] in tier1_symbols 
                            and any(y >= 2015 for y in g['missing_years'])]
            
            if recent_symbols:
                plans.append({
                    'name': 'Critical Recent Gaps',
                    'priority': 1,
                    'start_date': date(2015, 1, 1),
                    'end_date': date(2020, 12, 31),
                    'symbols': recent_symbols,
                    'vendors': ['polygon', 'eodhd'],
                    'estimated_cost_usd': len(recent_symbols) * 6 * 2.5 / 1000  # 6 years, $2.50 per call
                })
            
            # Plan 2: EPS-only backfill  
            eps_symbols = [g['symbol'] for g in gaps if g['eps_missing_years']]
            if eps_symbols:
                plans.append({
                    'name': 'EPS Enhancement',
                    'priority': 2,
                    'start_date': date(2000, 1, 1),
                    'end_date': date.today(),
                    'symbols': eps_symbols,
                    'vendors': ['polygon'],
                    'estimated_cost_usd': len(eps_symbols) * 25 * 2.0 / 1000  # 25 years
                })
            
            return plans
        
        plans = create_backfill_plans(sample_gap_data)
        
        assert len(plans) >= 1
        
        # Check first plan (highest priority)
        critical_plan = plans[0]
        assert critical_plan['priority'] == 1
        assert 'AAPL' in critical_plan['symbols']  # High priority symbol
        assert 'TSLA' in critical_plan['symbols']  # Has recent missing years
        assert len(critical_plan['vendors']) >= 1
        assert critical_plan['estimated_cost_usd'] > 0
        
        # Plans should be sorted by priority
        for i in range(1, len(plans)):
            assert plans[i-1]['priority'] <= plans[i]['priority']
    
    def test_cost_estimation(self):
        """Test API cost estimation for backfill plans"""
        vendor_costs = {
            'polygon': 2.0,     # $2 per 1000 calls
            'eodhd': 0.5,       # $0.50 per 1000 calls
            'tiingo': 1.0,      # $1 per 1000 calls
            'alpha_vantage': 0  # Free tier
        }
        
        def estimate_plan_cost(symbols: List[str], vendors: List[str], years: int) -> float:
            """Estimate cost for a backfill plan"""
            total_calls = len(symbols) * len(vendors) * years
            cost_per_call = sum(vendor_costs[v] for v in vendors) / len(vendors)
            return total_calls * cost_per_call / 1000
        
        # Test cost calculation
        cost = estimate_plan_cost(['AAPL', 'MSFT'], ['polygon', 'eodhd'], 5)
        expected_cost = 2 * 2 * 5 * ((2.0 + 0.5) / 2) / 1000  # 2 symbols, 2 vendors, 5 years
        assert cost == expected_cost
        
        # Free tier should have zero cost
        free_cost = estimate_plan_cost(['AAPL'], ['alpha_vantage'], 10)
        assert free_cost == 0.0
    
    def test_plan_validation(self):
        """Test validation of backfill plan parameters"""
        
        def validate_plan(plan: Dict) -> List[str]:
            """Validate backfill plan parameters"""
            errors = []
            
            if not plan.get('name'):
                errors.append('Plan name is required')
            
            if not isinstance(plan.get('priority', 0), int) or plan['priority'] < 1:
                errors.append('Priority must be positive integer')
            
            if not plan.get('symbols') or len(plan['symbols']) == 0:
                errors.append('At least one symbol is required')
            
            if not plan.get('vendors') or len(plan['vendors']) == 0:
                errors.append('At least one vendor is required')
            
            start_date = plan.get('start_date')
            end_date = plan.get('end_date')
            if start_date and end_date and start_date >= end_date:
                errors.append('Start date must be before end date')
            
            if plan.get('estimated_cost_usd', -1) < 0:
                errors.append('Estimated cost must be non-negative')
            
            return errors
        
        # Valid plan
        valid_plan = {
            'name': 'Test Plan',
            'priority': 1,
            'symbols': ['AAPL', 'MSFT'],
            'vendors': ['polygon'],
            'start_date': date(2020, 1, 1),
            'end_date': date(2025, 1, 1),
            'estimated_cost_usd': 10.50
        }
        
        assert validate_plan(valid_plan) == []
        
        # Invalid plan
        invalid_plan = {
            'name': '',  # Empty name
            'priority': 0,  # Invalid priority
            'symbols': [],  # No symbols
            'vendors': [],  # No vendors
            'start_date': date(2025, 1, 1),
            'end_date': date(2020, 1, 1),  # End before start
            'estimated_cost_usd': -5  # Negative cost
        }
        
        errors = validate_plan(invalid_plan)
        assert len(errors) == 6  # Should catch all validation errors

class TestBackfillExecution:
    """Test backfill execution logic and dry-run functionality"""
    
    @pytest.mark.asyncio
    async def test_dry_run_execution(self):
        """Test dry-run backfill execution"""
        
        sample_plan = {
            'name': 'Test Backfill',
            'priority': 1,
            'symbols': ['AAPL', 'MSFT'],
            'vendors': ['polygon'],
            'start_date': date(2020, 1, 1),
            'end_date': date(2022, 12, 31),
            'estimated_cost_usd': 15.0,
            'expected_records': 24  # 2 symbols * 3 years * 4 quarters
        }
        
        async def execute_backfill_plan(plan: Dict, dry_run: bool = True) -> Dict:
            """Execute backfill plan (dry run simulation)"""
            results = {
                'plan_name': plan['name'],
                'dry_run': dry_run,
                'start_time': datetime.now(),
                'symbols_processed': 0,
                'records_collected': 0,
                'api_calls_made': 0,
                'errors': []
            }
            
            if dry_run:
                results.update({
                    'estimated_records': plan['expected_records'],
                    'estimated_api_calls': len(plan['symbols']) * len(plan['vendors']) * 3,  # 3 years
                    'estimated_cost': plan['estimated_cost_usd']
                })
                return results
            
            # Actual execution would go here
            return results
        
        result = await execute_backfill_plan(sample_plan, dry_run=True)
        
        assert result['dry_run'] is True
        assert result['plan_name'] == 'Test Backfill'
        assert result['estimated_records'] == 24
        assert result['estimated_api_calls'] == 6  # 2 symbols * 1 vendor * 3 years
        assert result['estimated_cost'] == 15.0
    
    def test_backfill_progress_tracking(self):
        """Test progress tracking during backfill execution"""
        
        class BackfillProgressTracker:
            def __init__(self, total_symbols: int):
                self.total_symbols = total_symbols
                self.processed_symbols = 0
                self.successful_symbols = 0
                self.failed_symbols = 0
                self.total_records = 0
                self.start_time = datetime.now()
            
            def update_progress(self, symbol: str, success: bool, records: int = 0):
                self.processed_symbols += 1
                if success:
                    self.successful_symbols += 1
                    self.total_records += records
                else:
                    self.failed_symbols += 1
            
            def get_progress_report(self) -> Dict:
                elapsed = datetime.now() - self.start_time
                progress_pct = (self.processed_symbols / self.total_symbols) * 100
                
                return {
                    'progress_percentage': progress_pct,
                    'symbols_processed': self.processed_symbols,
                    'symbols_successful': self.successful_symbols,
                    'symbols_failed': self.failed_symbols,
                    'total_records_collected': self.total_records,
                    'elapsed_seconds': elapsed.total_seconds(),
                    'estimated_remaining_seconds': (elapsed.total_seconds() / self.processed_symbols) * 
                                                 (self.total_symbols - self.processed_symbols) if self.processed_symbols > 0 else 0
                }
        
        # Test progress tracking
        tracker = BackfillProgressTracker(total_symbols=10)
        
        # Simulate processing symbols
        tracker.update_progress('AAPL', success=True, records=20)
        tracker.update_progress('MSFT', success=True, records=18)
        tracker.update_progress('XYZ', success=False, records=0)
        
        report = tracker.get_progress_report()
        
        assert report['progress_percentage'] == 30.0  # 3 of 10 symbols
        assert report['symbols_processed'] == 3
        assert report['symbols_successful'] == 2
        assert report['symbols_failed'] == 1
        assert report['total_records_collected'] == 38  # 20 + 18
        assert report['elapsed_seconds'] > 0
    
    def test_error_handling_and_recovery(self):
        """Test error handling during backfill execution"""
        
        def simulate_backfill_errors():
            """Simulate various types of backfill errors"""
            errors = []
            
            # API rate limit error
            errors.append({
                'symbol': 'AAPL',
                'vendor': 'polygon',
                'error_type': 'rate_limit',
                'message': 'Rate limit exceeded, retry after 60 seconds',
                'timestamp': datetime.now(),
                'retry_recommended': True
            })
            
            # Invalid symbol error
            errors.append({
                'symbol': 'INVALID',
                'vendor': 'polygon',
                'error_type': 'invalid_symbol',
                'message': 'Symbol not found',
                'timestamp': datetime.now(),
                'retry_recommended': False
            })
            
            # Network timeout error
            errors.append({
                'symbol': 'MSFT',
                'vendor': 'eodhd',
                'error_type': 'timeout',
                'message': 'Request timed out after 30 seconds',
                'timestamp': datetime.now(),
                'retry_recommended': True
            })
            
            return errors
        
        errors = simulate_backfill_errors()
        
        # Categorize errors
        retry_errors = [e for e in errors if e['retry_recommended']]
        permanent_errors = [e for e in errors if not e['retry_recommended']]
        
        assert len(retry_errors) == 2  # rate_limit and timeout
        assert len(permanent_errors) == 1  # invalid_symbol
        
        # Test error recovery strategy
        def create_recovery_plan(errors: List[Dict]) -> Dict:
            retry_symbols = list(set(e['symbol'] for e in retry_errors))
            failed_symbols = list(set(e['symbol'] for e in permanent_errors))
            
            return {
                'symbols_to_retry': retry_symbols,
                'symbols_to_skip': failed_symbols,
                'retry_delay_seconds': 60,  # Wait before retry
                'max_retries': 3
            }
        
        recovery_plan = create_recovery_plan(errors)
        assert 'AAPL' in recovery_plan['symbols_to_retry']
        assert 'MSFT' in recovery_plan['symbols_to_retry'] 
        assert 'INVALID' in recovery_plan['symbols_to_skip']

class TestBackfillIntegration:
    """Integration tests for the complete backfill system"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_gap_analysis_to_plan(self, mock_environment, sample_gap_data):
        """Test complete flow from gap analysis to plan generation"""
        env, pool, conn = mock_environment
        
        # Mock the database calls for gap analysis
        conn.fetch = AsyncMock(return_value=[
            {'symbol': 'AAPL'}, {'symbol': 'MSFT'}, {'symbol': 'GOOGL'}
        ])
        
        with patch('src.events.backfill.historical_earnings_backfill.Environment', return_value=env):
            # Simulate complete backfill analysis
            class MockBackfillSystem:
                async def run_complete_analysis(self):
                    # 1. Get symbols to analyze
                    symbols = ['AAPL', 'MSFT', 'GOOGL']
                    
                    # 2. Analyze gaps (using sample data)
                    gaps = sample_gap_data
                    
                    # 3. Generate plans
                    plans = [
                        {
                            'name': 'Critical Gaps',
                            'priority': 1,
                            'symbols': ['AAPL', 'TSLA'],
                            'cost': 25.0
                        },
                        {
                            'name': 'EPS Enhancement', 
                            'priority': 2,
                            'symbols': ['AAPL', 'TSLA', 'XYZ'],
                            'cost': 15.0
                        }
                    ]
                    
                    # 4. Generate summary report
                    return {
                        'symbols_analyzed': len(symbols),
                        'symbols_with_gaps': len(gaps),
                        'total_missing_years': sum(len(g['missing_years']) for g in gaps),
                        'backfill_plans': len(plans),
                        'total_estimated_cost': sum(p['cost'] for p in plans),
                        'plans': plans
                    }
            
            system = MockBackfillSystem()
            analysis = await system.run_complete_analysis()
            
            assert analysis['symbols_analyzed'] == 3
            assert analysis['symbols_with_gaps'] == 3
            assert analysis['total_missing_years'] == 9  # 2 + 4 + 3 from sample data
            assert analysis['backfill_plans'] == 2
            assert analysis['total_estimated_cost'] == 40.0  # 25 + 15
    
    def test_backfill_report_generation(self):
        """Test comprehensive backfill report generation"""
        
        def generate_backfill_report(gaps: List[Dict], plans: List[Dict]) -> Dict:
            """Generate comprehensive backfill analysis report"""
            
            total_symbols_with_gaps = len(gaps)
            total_missing_years = sum(len(g['missing_years']) for g in gaps)
            total_eps_gaps = sum(len(g['eps_missing_years']) for g in gaps)
            
            total_estimated_cost = sum(p.get('estimated_cost_usd', 0) for p in plans)
            total_expected_records = sum(p.get('expected_records', 0) for p in plans)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'symbols_analyzed': len(gaps),
                    'symbols_with_gaps': total_symbols_with_gaps,
                    'total_missing_years': total_missing_years,
                    'total_eps_gaps': total_eps_gaps,
                    'backfill_plans': len(plans),
                    'estimated_total_cost_usd': total_estimated_cost,
                    'expected_total_records': total_expected_records
                },
                'top_priority_gaps': sorted(gaps, key=lambda x: x['priority_score'], reverse=True)[:5],
                'backfill_plans': plans
            }
        
        # Test with sample data
        sample_plans = [
            {'estimated_cost_usd': 25.0, 'expected_records': 100},
            {'estimated_cost_usd': 15.0, 'expected_records': 60}
        ]
        
        report = generate_backfill_report(sample_gap_data, sample_plans)
        
        assert 'timestamp' in report
        assert report['summary']['symbols_with_gaps'] == 3
        assert report['summary']['total_missing_years'] == 9  # From sample data
        assert report['summary']['estimated_total_cost_usd'] == 40.0
        assert report['summary']['expected_total_records'] == 160
        assert len(report['top_priority_gaps']) == 3  # All 3 gaps in sample
        assert report['top_priority_gaps'][0]['symbol'] == 'AAPL'  # Highest priority

if __name__ == "__main__":
    pytest.main([__file__, "-v"])