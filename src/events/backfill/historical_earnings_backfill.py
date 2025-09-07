#!/usr/bin/env python3
"""
Historical Earnings Data Backfill System

Systematic backfill of historical earnings data gaps using multiple vendor strategies.
Prioritizes high-value symbols and recent history for maximum impact.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
import json
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.environment import Environment
from core.logging.logger_config import get_logger

logger = get_logger(__name__)

@dataclass
class BackfillPlan:
    """Backfill execution plan"""
    name: str
    priority: int
    start_date: date
    end_date: date
    symbols: List[str]
    vendors: List[str]
    estimated_api_calls: int
    estimated_cost_usd: float
    expected_records: int

@dataclass
class GapAnalysis:
    """Coverage gap analysis"""
    symbol: str
    missing_years: List[int]
    incomplete_years: List[int]  # Years with <4 quarters
    eps_missing_years: List[int]
    priority_score: float  # Higher = more important to fill

class HistoricalEarningsBackfill:
    """Historical earnings data backfill system"""
    
    def __init__(self):
        self.env = Environment()
        
        # Symbol prioritization (market cap based)
        self.symbol_priorities = {
            'tier_1': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK.B'],  # Mega cap
            'tier_2': ['JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'ADBE', 'NFLX'],  # Large cap leaders  
            'tier_3': ['CRM', 'ORCL', 'CSCO', 'INTC', 'IBM', 'QCOM', 'AMD', 'TXN'],  # Tech large caps
            'tier_4': ['WMT', 'KO', 'PFE', 'MRK', 'T', 'VZ', 'CMCSA', 'PEP', 'ABT'],  # Consumer/pharma
            'sp500': []  # Will be populated from database
        }
        
        # Vendor API cost estimates (per 1000 calls)
        self.vendor_costs = {
            'polygon': 2.0,    # $2 per 1000 calls
            'eodhd': 0.5,      # $0.50 per 1000 calls  
            'tiingo': 1.0,     # $1 per 1000 calls
            'alpha_vantage': 0  # Free tier
        }
    
    async def analyze_coverage_gaps(self, symbols: Optional[List[str]] = None) -> List[GapAnalysis]:
        """Analyze coverage gaps for symbols"""
        if symbols is None:
            # Get major symbols from database
            symbols = await self._get_major_symbols()
        
        gaps = []
        
        gap_query = f"""
        WITH yearly_coverage AS (
            SELECT 
                symbol,
                EXTRACT(YEAR FROM report_period) as year,
                COUNT(*) as earnings_count,
                COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_count
            FROM {self.env.get_table_name('earnings_events')} 
            WHERE symbol = $1
              AND report_period >= '2000-01-01'
            GROUP BY symbol, EXTRACT(YEAR FROM report_period)
        ),
        year_range AS (
            SELECT generate_series(2000, EXTRACT(YEAR FROM CURRENT_DATE)::int) as year
        )
        SELECT 
            yr.year,
            COALESCE(yc.earnings_count, 0) as earnings_count,
            COALESCE(yc.eps_count, 0) as eps_count
        FROM year_range yr
        LEFT JOIN yearly_coverage yc ON yr.year = yc.year
        ORDER BY yr.year
        """
        
        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                for symbol in symbols:
                    rows = await conn.fetch(gap_query, symbol)
                    
                    missing_years = []
                    incomplete_years = []
                    eps_missing_years = []
                    
                    for row in rows:
                        year = row['year']
                        earnings_count = row['earnings_count']
                        eps_count = row['eps_count']
                        
                        if earnings_count == 0:
                            missing_years.append(year)
                        elif earnings_count < 4:  # Expected 4 quarters
                            incomplete_years.append(year)
                        
                        if eps_count == 0 and earnings_count > 0:
                            eps_missing_years.append(year)
                    
                    # Calculate priority score
                    priority_score = self._calculate_priority_score(
                        symbol, missing_years, incomplete_years, eps_missing_years
                    )
                    
                    if missing_years or incomplete_years or eps_missing_years:
                        gaps.append(GapAnalysis(
                            symbol=symbol,
                            missing_years=missing_years,
                            incomplete_years=incomplete_years,
                            eps_missing_years=eps_missing_years,
                            priority_score=priority_score
                        ))
        
        # Sort by priority (highest first)
        gaps.sort(key=lambda x: x.priority_score, reverse=True)
        return gaps
    
    def _calculate_priority_score(self, symbol: str, missing_years: List[int], 
                                 incomplete_years: List[int], eps_missing_years: List[int]) -> float:
        """Calculate priority score for backfill"""
        score = 0.0
        
        # Symbol tier weighting
        if symbol in self.symbol_priorities['tier_1']:
            score += 100
        elif symbol in self.symbol_priorities['tier_2']:
            score += 80
        elif symbol in self.symbol_priorities['tier_3']:
            score += 60
        elif symbol in self.symbol_priorities['tier_4']:
            score += 40
        else:
            score += 20
        
        # Recent years are more valuable
        current_year = datetime.now().year
        for year in missing_years:
            age = current_year - year
            if age <= 5:
                score += 50 - (age * 8)  # 50, 42, 34, 26, 18 points
            elif age <= 10:
                score += 20 - (age - 5) * 2  # 10-20 points
            else:
                score += max(1, 10 - (age - 10))  # 1-10 points
        
        # Incomplete years penalty/bonus
        for year in incomplete_years:
            age = current_year - year
            if age <= 5:
                score += 10 - age  # 10, 9, 8, 7, 6 points
            else:
                score += 2  # Small bonus for older incomplete years
        
        # EPS missing bonus
        for year in eps_missing_years:
            age = current_year - year
            if age <= 5:
                score += 5  # Recent EPS data is valuable
            else:
                score += 2
        
        return score
    
    async def _get_major_symbols(self, limit: int = 500) -> List[str]:
        """Get major symbols from database"""
        query = f"""
        SELECT DISTINCT symbol 
        FROM {self.env.get_table_name('instruments')}
        WHERE active = true
          AND exchange IN ('NYSE', 'NASDAQ')
          AND symbol ~ '^[A-Z]{{1,5}}$'
        ORDER BY symbol
        LIMIT $1
        """
        
        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, limit)
                return [row['symbol'] for row in rows]
    
    def create_backfill_plans(self, gaps: List[GapAnalysis]) -> List[BackfillPlan]:
        """Create prioritized backfill plans"""
        plans = []
        
        # Plan 1: Recent critical gaps (Tier 1 symbols, 2015-2020)
        tier1_symbols = [g.symbol for g in gaps if g.symbol in self.symbol_priorities['tier_1']]
        recent_tier1 = [s for s in tier1_symbols if any(y >= 2015 for y in gaps[0].missing_years)]
        
        if recent_tier1:
            plans.append(BackfillPlan(
                name="Critical Recent Gaps (Tier 1)",
                priority=1,
                start_date=date(2015, 1, 1),
                end_date=date(2020, 12, 31),
                symbols=recent_tier1[:20],  # Limit to most critical
                vendors=['polygon', 'eodhd'],
                estimated_api_calls=len(recent_tier1[:20]) * 2 * 6,  # 2 vendors, 6 years
                estimated_cost_usd=len(recent_tier1[:20]) * 2 * 6 * (2.0 + 0.5) / 1000,
                expected_records=len(recent_tier1[:20]) * 6 * 4  # 6 years * 4 quarters
            ))
        
        # Plan 2: High-priority symbol gaps (Tier 1+2, 2010-2015)
        tier12_symbols = [g.symbol for g in gaps if g.symbol in self.symbol_priorities['tier_1'] + self.symbol_priorities['tier_2']]
        
        if tier12_symbols:
            plans.append(BackfillPlan(
                name="High Priority Historical (Tier 1-2)",
                priority=2,
                start_date=date(2010, 1, 1), 
                end_date=date(2015, 12, 31),
                symbols=tier12_symbols[:50],
                vendors=['eodhd'],  # Better historical coverage, lower cost
                estimated_api_calls=len(tier12_symbols[:50]) * 6,  # 6 years
                estimated_cost_usd=len(tier12_symbols[:50]) * 6 * 0.5 / 1000,
                expected_records=len(tier12_symbols[:50]) * 6 * 4
            ))
        
        # Plan 3: EPS-only backfill (existing earnings, missing EPS)
        eps_only_symbols = [g.symbol for g in gaps if g.eps_missing_years and not g.missing_years]
        
        if eps_only_symbols:
            plans.append(BackfillPlan(
                name="EPS Data Enhancement",
                priority=3,
                start_date=date(2000, 1, 1),
                end_date=date.today(),
                symbols=eps_only_symbols[:100],
                vendors=['polygon'],  # Best EPS data quality
                estimated_api_calls=len(eps_only_symbols[:100]) * 25,  # 25 years
                estimated_cost_usd=len(eps_only_symbols[:100]) * 25 * 2.0 / 1000,
                expected_records=len(eps_only_symbols[:100]) * 25 * 4
            ))
        
        # Plan 4: Broad coverage (All gaps, 2000-2010)
        all_gap_symbols = [g.symbol for g in gaps]
        
        if all_gap_symbols:
            plans.append(BackfillPlan(
                name="Comprehensive Historical Coverage",
                priority=4,
                start_date=date(2000, 1, 1),
                end_date=date(2010, 12, 31),
                symbols=all_gap_symbols[:200],
                vendors=['eodhd'],
                estimated_api_calls=len(all_gap_symbols[:200]) * 11,  # 11 years
                estimated_cost_usd=len(all_gap_symbols[:200]) * 11 * 0.5 / 1000,
                expected_records=len(all_gap_symbols[:200]) * 11 * 4
            ))
        
        return plans
    
    async def execute_backfill_plan(self, plan: BackfillPlan, dry_run: bool = True) -> Dict:
        """Execute a backfill plan"""
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Executing backfill plan: {plan.name}")
        logger.info(f"Symbols: {len(plan.symbols)}, Date range: {plan.start_date} to {plan.end_date}")
        logger.info(f"Vendors: {plan.vendors}, Estimated cost: ${plan.estimated_cost_usd:.2f}")
        
        results = {
            'plan_name': plan.name,
            'dry_run': dry_run,
            'start_time': datetime.now(),
            'symbols_processed': 0,
            'records_collected': 0,
            'records_stored': 0,
            'api_calls_made': 0,
            'errors': [],
            'vendor_results': {}
        }
        
        if dry_run:
            logger.info("DRY RUN: Would execute the following actions:")
            for vendor in plan.vendors:
                logger.info(f"  - {vendor}: {len(plan.symbols)} symbols x {(plan.end_date - plan.start_date).days // 365 + 1} years")
            
            results['estimated_records'] = plan.expected_records
            results['estimated_api_calls'] = plan.estimated_api_calls
            results['estimated_cost'] = plan.estimated_cost_usd
            return results
        
        # TODO: Implement actual backfill execution
        # This would require:
        # 1. Initialize vendor adapters with API keys
        # 2. For each symbol and vendor combination:
        #    - Fetch earnings data for date range
        #    - Parse and validate data
        #    - Store in database with conflict resolution
        # 3. Track progress and handle rate limits
        # 4. Generate completion report
        
        logger.warning("Actual backfill execution not implemented yet - use dry_run=True")
        return results
    
    async def generate_backfill_report(self) -> Dict:
        """Generate comprehensive backfill analysis and recommendations"""
        logger.info("Generating historical earnings backfill analysis...")
        
        # Analyze gaps
        gaps = await self.analyze_coverage_gaps()
        
        # Create plans
        plans = self.create_backfill_plans(gaps)
        
        # Summary statistics
        total_symbols_with_gaps = len(gaps)
        total_missing_years = sum(len(g.missing_years) for g in gaps)
        total_eps_gaps = sum(len(g.eps_missing_years) for g in gaps)
        
        # Cost estimates
        total_estimated_cost = sum(p.estimated_cost_usd for p in plans)
        total_expected_records = sum(p.expected_records for p in plans)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'symbols_analyzed': len(gaps) if gaps else 0,
                'symbols_with_gaps': total_symbols_with_gaps,
                'total_missing_years': total_missing_years,
                'total_eps_gaps': total_eps_gaps,
                'backfill_plans': len(plans),
                'estimated_total_cost_usd': total_estimated_cost,
                'expected_total_records': total_expected_records
            },
            'top_priority_gaps': [
                {
                    'symbol': g.symbol,
                    'priority_score': g.priority_score,
                    'missing_years': g.missing_years,
                    'incomplete_years': g.incomplete_years,
                    'eps_missing_years': g.eps_missing_years
                }
                for g in gaps[:20]  # Top 20 priority gaps
            ],
            'backfill_plans': [
                {
                    'name': p.name,
                    'priority': p.priority,
                    'date_range': f"{p.start_date} to {p.end_date}",
                    'symbols_count': len(p.symbols),
                    'vendors': p.vendors,
                    'estimated_cost_usd': p.estimated_cost_usd,
                    'expected_records': p.expected_records,
                    'sample_symbols': p.symbols[:10]  # First 10 symbols as sample
                }
                for p in plans
            ]
        }
        
        return report

async def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Historical Earnings Data Backfill")
    parser.add_argument('--analyze', action='store_true', help='Analyze coverage gaps')
    parser.add_argument('--plan', action='store_true', help='Generate backfill plans')
    parser.add_argument('--execute', type=str, help='Execute specific plan by name')
    parser.add_argument('--dry-run', action='store_true', help='Dry run execution')
    parser.add_argument('--symbols', type=str, help='Comma-separated symbols to analyze')
    
    args = parser.parse_args()
    
    backfill = HistoricalEarningsBackfill()
    
    if args.analyze or args.plan:
        report = await backfill.generate_backfill_report()
        print(json.dumps(report, indent=2, default=str))
    
    elif args.execute:
        # Generate plans and find the requested one
        gaps = await backfill.analyze_coverage_gaps()
        plans = backfill.create_backfill_plans(gaps)
        
        target_plan = None
        for plan in plans:
            if plan.name == args.execute:
                target_plan = plan
                break
        
        if target_plan:
            results = await backfill.execute_backfill_plan(target_plan, dry_run=args.dry_run)
            print(json.dumps(results, indent=2, default=str))
        else:
            print(f"Plan '{args.execute}' not found. Available plans:")
            for plan in plans:
                print(f"  - {plan.name}")
    
    else:
        # Default: show analysis
        await backfill.generate_backfill_report()

if __name__ == "__main__":
    asyncio.run(main())