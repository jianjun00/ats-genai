#!/usr/bin/env python3
"""
Majority Voting Price Data Reconciler

This module implements majority voting logic to reconcile price discrepancies 
across multiple data vendors (Polygon, Tiingo, Alpha Vantage, FMP).

Key Features:
- Identify price discrepancies between vendors
- Use statistical analysis to determine consensus prices
- Flag outlier vendors for investigation
- Generate reconciliation reports
- Support tie-breaking with Alpha Vantage/FMP when primary vendors disagree
"""

import asyncio
import asyncpg
import logging
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from statistics import median
from enum import Enum

# Add src to path for imports
from config.environment import Environment

class ReconciliationDecision(Enum):
    CONSENSUS = "consensus"
    MAJORITY_RULE = "majority_rule"
    MEDIAN_FALLBACK = "median_fallback"
    INSUFFICIENT_DATA = "insufficient_data"
    HIGH_VARIANCE = "high_variance"

@dataclass
class VendorPrice:
    vendor: str
    date: date
    symbol: str
    close_price: float
    volume: int
    adj_close: Optional[float] = None

@dataclass
class PriceConsensus:
    date: date
    symbol: str
    consensus_price: float
    consensus_volume: int
    decision_method: ReconciliationDecision
    vendor_prices: List[VendorPrice]
    price_variance: float
    confidence_score: float
    outlier_vendors: List[str]
    notes: str

class MajorityVotingReconciler:
    """
    Reconciles price data across multiple vendors using majority voting and statistical analysis
    """
    
    def __init__(self, env: Environment, 
                 max_price_variance: float = 0.05,  # 5% max variance
                 min_vendors_for_consensus: int = 2,
                 outlier_threshold: float = 2.0):  # 2 standard deviations
        self.env = env
        self.db_url = env.get_database_url()
        self.max_price_variance = max_price_variance
        self.min_vendors_for_consensus = min_vendors_for_consensus
        self.outlier_threshold = outlier_threshold
        self.logger = logging.getLogger(__name__)
        
        # Define vendor priority for tie-breaking
        self.vendor_priority = {
            'polygon': 1,    # Primary
            'tiingo': 2,     # Primary  
            'fmp': 3,        # Tie-breaker
            'alphavantage': 4  # Tie-breaker
        }
    
    async def get_multi_vendor_prices(self, symbol: str, start_date: date, end_date: date) -> List[VendorPrice]:
        """Fetch prices for a symbol from all vendors"""
        
        vendor_tables = {
            'polygon': self.env.get_table_name('daily_prices_polygon'),
            'tiingo': self.env.get_table_name('daily_prices_tiingo'),
            'alphavantage': self.env.get_table_name('daily_prices_alphavantage'),
            'fmp': self.env.get_table_name('daily_prices_fmp')
        }
        
        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)
        all_prices = []
        
        try:
            async with pool.acquire() as conn:
                # Get instrument_id for symbol
                instrument_id = await conn.fetchval("""
                    SELECT id FROM dev_instruments WHERE symbol = $1
                """, symbol)
                
                if not instrument_id:
                    self.logger.warning(f"Symbol {symbol} not found in instruments table")
                    return []
                
                # Fetch from each vendor
                for vendor, table_name in vendor_tables.items():
                    try:
                        rows = await conn.fetch(f"""
                            SELECT date, close, volume, adj_close
                            FROM {table_name}
                            WHERE instrument_id = $1 
                              AND date BETWEEN $2 AND $3
                            ORDER BY date
                        """, instrument_id, start_date, end_date)
                        
                        for row in rows:
                            vendor_price = VendorPrice(
                                vendor=vendor,
                                date=row['date'],
                                symbol=symbol,
                                close_price=float(row['close']),
                                volume=int(row['volume']) if row['volume'] else 0,
                                adj_close=float(row['adj_close']) if row['adj_close'] else None
                            )
                            all_prices.append(vendor_price)
                            
                    except Exception as e:
                        self.logger.warning(f"Error fetching {vendor} data for {symbol}: {e}")
                        continue
        
        finally:
            await pool.close()
        
        return all_prices
    
    def group_prices_by_date(self, prices: List[VendorPrice]) -> Dict[date, List[VendorPrice]]:
        """Group prices by date for day-by-day reconciliation"""
        grouped = {}
        for price in prices:
            if price.date not in grouped:
                grouped[price.date] = []
            grouped[price.date].append(price)
        return grouped
    
    def calculate_price_statistics(self, prices: List[VendorPrice]) -> Dict[str, float]:
        """Calculate statistical measures for price analysis"""
        if not prices:
            return {}
        
        close_prices = [p.close_price for p in prices]
        volumes = [p.volume for p in prices if p.volume > 0]
        
        stats = {
            'mean_price': np.mean(close_prices),
            'median_price': np.median(close_prices),
            'std_price': np.std(close_prices) if len(close_prices) > 1 else 0.0,
            'price_range': max(close_prices) - min(close_prices),
            'coefficient_of_variation': np.std(close_prices) / np.mean(close_prices) if np.mean(close_prices) > 0 else 0.0,
            'total_vendors': len(prices)
        }
        
        if volumes:
            stats['median_volume'] = np.median(volumes)
            stats['total_volume'] = sum(volumes)
        
        return stats
    
    def identify_outliers(self, prices: List[VendorPrice], stats: Dict[str, float]) -> List[str]:
        """Identify outlier vendors using statistical analysis and percentage deviation"""
        if len(prices) < 3:
            return []
        
        outliers = []
        mean_price = stats['mean_price']
        median_price = stats['median_price']
        std_price = stats['std_price']
        
        for price in prices:
            # Use both z-score and percentage deviation for robust outlier detection
            z_score = abs(price.close_price - mean_price) / std_price if std_price > 0 else 0
            pct_deviation = abs(price.close_price - median_price) / median_price if median_price > 0 else 0
            
            # Flag as outlier if either z-score is high OR percentage deviation is extreme
            is_statistical_outlier = z_score > 1.5 and std_price > 0  # Lower z-score threshold
            is_percentage_outlier = pct_deviation > 0.05  # 5% deviation from median (tighter)
            
            if is_statistical_outlier or is_percentage_outlier:
                outliers.append(price.vendor)
        
        return outliers
    
    def determine_consensus_price(self, prices: List[VendorPrice]) -> PriceConsensus:
        """Determine consensus price using majority voting logic"""
        
        if not prices:
            return PriceConsensus(
                date=date.today(),
                symbol="UNKNOWN",
                consensus_price=0.0,
                consensus_volume=0,
                decision_method=ReconciliationDecision.INSUFFICIENT_DATA,
                vendor_prices=[],
                price_variance=0.0,
                confidence_score=0.0,
                outlier_vendors=[],
                notes="No price data available"
            )
        
        stats = self.calculate_price_statistics(prices)
        outliers = self.identify_outliers(prices, stats)
        
        # Filter out outliers for consensus calculation
        non_outlier_prices = [p for p in prices if p.vendor not in outliers]
        
        if len(non_outlier_prices) < self.min_vendors_for_consensus:
            # Fall back to all prices if too many outliers
            non_outlier_prices = prices
            outliers = []
        
        # Calculate consensus
        consensus_method = ReconciliationDecision.INSUFFICIENT_DATA
        consensus_price = 0.0
        consensus_volume = 0
        confidence_score = 0.0
        notes = ""
        
        if len(non_outlier_prices) >= 3:
            # Strong majority - use median of non-outliers
            consensus_price = median([p.close_price for p in non_outlier_prices])
            consensus_volume = int(median([p.volume for p in non_outlier_prices if p.volume > 0]))
            consensus_method = ReconciliationDecision.CONSENSUS
            confidence_score = 0.9
            notes = f"Strong consensus from {len(non_outlier_prices)} vendors"
            
        elif len(non_outlier_prices) == 2:
            # Two vendors agree - use average
            price_values = [p.close_price for p in non_outlier_prices]
            if abs(price_values[0] - price_values[1]) / max(price_values) <= self.max_price_variance:
                consensus_price = np.mean(price_values)
                consensus_volume = int(np.mean([p.volume for p in non_outlier_prices if p.volume > 0]))
                consensus_method = ReconciliationDecision.MAJORITY_RULE
                confidence_score = 0.7
                notes = f"Two-vendor agreement within {self.max_price_variance*100:.1f}% tolerance"
            else:
                # Use tie-breaking with vendor priority
                sorted_prices = sorted(non_outlier_prices, key=lambda p: self.vendor_priority.get(p.vendor, 99))
                consensus_price = sorted_prices[0].close_price
                consensus_volume = sorted_prices[0].volume
                consensus_method = ReconciliationDecision.MEDIAN_FALLBACK
                confidence_score = 0.5
                notes = f"Used priority vendor {sorted_prices[0].vendor} for tie-breaking"
        
        else:
            # Single vendor or insufficient data - use what we have
            consensus_price = prices[0].close_price
            consensus_volume = prices[0].volume
            consensus_method = ReconciliationDecision.INSUFFICIENT_DATA
            confidence_score = 0.3
            notes = f"Insufficient vendor data, using {prices[0].vendor}"
        
        # Calculate final variance
        price_variance = stats.get('coefficient_of_variation', 0.0)
        
        # Adjust confidence based on variance
        if price_variance > self.max_price_variance:
            confidence_score *= 0.7
            if consensus_method != ReconciliationDecision.HIGH_VARIANCE:
                notes += f" (High variance: {price_variance:.3f})"
        
        return PriceConsensus(
            date=prices[0].date,
            symbol=prices[0].symbol,
            consensus_price=consensus_price,
            consensus_volume=consensus_volume,
            decision_method=consensus_method,
            vendor_prices=prices,
            price_variance=price_variance,
            confidence_score=confidence_score,
            outlier_vendors=outliers,
            notes=notes
        )
    
    async def reconcile_symbol_prices(self, symbol: str, start_date: date, end_date: date) -> List[PriceConsensus]:
        """Reconcile prices for a symbol across date range"""
        
        self.logger.info(f"Reconciling prices for {symbol} from {start_date} to {end_date}")
        
        # Get all vendor prices
        all_prices = await self.get_multi_vendor_prices(symbol, start_date, end_date)
        
        if not all_prices:
            self.logger.warning(f"No price data found for {symbol}")
            return []
        
        # Group by date
        grouped_prices = self.group_prices_by_date(all_prices)
        
        # Reconcile each date
        reconciliations = []
        for trade_date, day_prices in sorted(grouped_prices.items()):
            consensus = self.determine_consensus_price(day_prices)
            reconciliations.append(consensus)
        
        return reconciliations
    
    async def generate_reconciliation_report(self, reconciliations: List[PriceConsensus]) -> str:
        """Generate human-readable reconciliation report"""
        
        if not reconciliations:
            return "No reconciliation data available"
        
        symbol = reconciliations[0].symbol
        total_days = len(reconciliations)
        
        # Count decision methods
        method_counts = {}
        confidence_scores = []
        high_variance_days = 0
        outlier_counts = {}
        
        for recon in reconciliations:
            method = recon.decision_method.value
            method_counts[method] = method_counts.get(method, 0) + 1
            confidence_scores.append(recon.confidence_score)
            
            if recon.price_variance > self.max_price_variance:
                high_variance_days += 1
                
            for outlier in recon.outlier_vendors:
                outlier_counts[outlier] = outlier_counts.get(outlier, 0) + 1
        
        avg_confidence = np.mean(confidence_scores) if confidence_scores else 0.0
        
        report = f"""
🔄 PRICE RECONCILIATION REPORT FOR {symbol}
{'='*60}

📊 SUMMARY STATISTICS:
  • Total trading days: {total_days}
  • Average confidence score: {avg_confidence:.2f}
  • High variance days: {high_variance_days} ({high_variance_days/total_days*100:.1f}%)

📋 DECISION METHOD BREAKDOWN:
"""
        
        for method, count in method_counts.items():
            pct = count / total_days * 100
            report += f"  • {method.replace('_', ' ').title()}: {count} days ({pct:.1f}%)\n"
        
        if outlier_counts:
            report += f"\n⚠️  OUTLIER VENDOR ANALYSIS:\n"
            for vendor, count in outlier_counts.items():
                pct = count / total_days * 100
                report += f"  • {vendor}: {count} outlier days ({pct:.1f}%)\n"
        
        # Show sample discrepancies
        high_variance_samples = [r for r in reconciliations if r.price_variance > self.max_price_variance][:5]
        if high_variance_samples:
            report += f"\n🚨 SAMPLE HIGH-VARIANCE DAYS:\n"
            for recon in high_variance_samples:
                report += f"  • {recon.date}: {recon.price_variance:.3f} variance, {len(recon.vendor_prices)} vendors\n"
                for vp in recon.vendor_prices:
                    report += f"    - {vp.vendor}: ${vp.close_price:.2f}\n"
                report += f"    → Consensus: ${recon.consensus_price:.2f} ({recon.decision_method.value})\n\n"
        
        report += f"\n✅ RECOMMENDATION:\n"
        if avg_confidence >= 0.8:
            report += "  High confidence in price data quality. Vendor consensus is strong.\n"
        elif avg_confidence >= 0.6:
            report += "  Moderate confidence. Consider investigating high-variance days.\n"
        else:
            report += "  Low confidence. Manual review recommended for price accuracy.\n"
        
        return report
    
    async def create_consensus_table(self, table_suffix: str = "consensus") -> str:
        """Create consensus price table with reconciled data"""
        
        consensus_table = self.env.get_table_name(f'daily_prices_{table_suffix}')
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {consensus_table} (
                        id SERIAL PRIMARY KEY,
                        instrument_id INTEGER NOT NULL,
                        date DATE NOT NULL,
                        consensus_price DECIMAL(10, 4) NOT NULL,
                        consensus_volume BIGINT,
                        decision_method VARCHAR(50) NOT NULL,
                        confidence_score DECIMAL(4, 3) NOT NULL,
                        price_variance DECIMAL(6, 5) NOT NULL,
                        vendor_count INTEGER NOT NULL,
                        outlier_vendors TEXT[],
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(instrument_id, date),
                        FOREIGN KEY (instrument_id) REFERENCES dev_instruments(id)
                    )
                """)
                
                # Create index for performance
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{consensus_table}_instrument_date 
                    ON {consensus_table}(instrument_id, date)
                """)
                
                self.logger.info(f"Created consensus table: {consensus_table}")
                return consensus_table
                
        finally:
            await pool.close()
    
    async def save_consensus_prices(self, reconciliations: List[PriceConsensus], 
                                  consensus_table: str) -> int:
        """Save reconciled consensus prices to database"""
        
        if not reconciliations:
            return 0
        
        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)
        try:
            async with pool.acquire() as conn:
                # Get instrument_id for symbol
                symbol = reconciliations[0].symbol
                instrument_id = await conn.fetchval("""
                    SELECT id FROM dev_instruments WHERE symbol = $1
                """, symbol)
                
                if not instrument_id:
                    self.logger.error(f"Instrument not found for symbol: {symbol}")
                    return 0
                
                # Prepare batch insert data
                insert_data = []
                for recon in reconciliations:
                    insert_data.append((
                        instrument_id,
                        recon.date,
                        recon.consensus_price,
                        recon.consensus_volume,
                        recon.decision_method.value,
                        recon.confidence_score,
                        recon.price_variance,
                        len(recon.vendor_prices),
                        recon.outlier_vendors,
                        recon.notes
                    ))
                
                # Batch insert with upsert
                await conn.executemany(f"""
                    INSERT INTO {consensus_table} 
                    (instrument_id, date, consensus_price, consensus_volume, 
                     decision_method, confidence_score, price_variance, vendor_count,
                     outlier_vendors, notes)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (instrument_id, date) DO UPDATE SET
                        consensus_price = EXCLUDED.consensus_price,
                        consensus_volume = EXCLUDED.consensus_volume,
                        decision_method = EXCLUDED.decision_method,
                        confidence_score = EXCLUDED.confidence_score,
                        price_variance = EXCLUDED.price_variance,
                        vendor_count = EXCLUDED.vendor_count,
                        outlier_vendors = EXCLUDED.outlier_vendors,
                        notes = EXCLUDED.notes,
                        updated_at = NOW()
                """, insert_data)
                
                return len(insert_data)
                
        finally:
            await pool.close()

async def main():
    """Example usage of majority voting reconciler"""
    
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    env = Environment()
    reconciler = MajorityVotingReconciler(env)
    
    # Example: reconcile AAPL prices for last month
    symbol = "AAPL"
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    
    print(f"🔄 Starting majority voting reconciliation for {symbol}")
    print(f"📅 Date range: {start_date} to {end_date}")
    
    # Perform reconciliation
    reconciliations = await reconciler.reconcile_symbol_prices(symbol, start_date, end_date)
    
    if reconciliations:
        # Generate report
        report = await reconciler.generate_reconciliation_report(reconciliations)
        print(report)
        
        # Create consensus table and save results
        consensus_table = await reconciler.create_consensus_table()
        saved_count = await reconciler.save_consensus_prices(reconciliations, consensus_table)
        
        print(f"✅ Saved {saved_count} consensus price records to {consensus_table}")
    else:
        print(f"❌ No reconciliation results for {symbol}")

if __name__ == "__main__":
    asyncio.run(main())