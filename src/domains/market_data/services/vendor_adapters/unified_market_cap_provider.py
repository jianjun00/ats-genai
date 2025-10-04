"""
Unified Market Cap Provider

Combines fundamental data and daily price data to provide unified market cap
calculations for each instrument and date. This provider:

1. Integrates UnifiedFundamentalProvider for fundamental market cap data
2. Uses UnifiedDailyPrice data to calculate market cap from shares outstanding
3. Performs cross-source validation and reconciliation
4. Provides confidence scoring and outlier detection
5. Handles missing data gracefully with multiple fallback methods

Market cap can be calculated via:
- Direct from fundamental data (reported market cap)
- Price * shares outstanding (from shares data)
- Cross-vendor validation and consensus building
"""

from typing import Dict, List, Optional, Any
from datetime import date, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncpg
import logging
import statistics

from core.platform.config.environment import Environment
from domains.market_data.services.fundamentals.unified_fundamental_provider import (
    UnifiedFundamentalProvider
)
from domains.market_data.services.eod.unified_daily_price_validator import (
    UnifiedDailyPriceValidator
)


class MarketCapValidationStatus(Enum):
    """Validation status for unified market cap data"""
    CONSENSUS = "consensus"                          # Multiple sources agree
    MAJORITY_CONSENSUS = "majority_consensus"       # Majority of sources agree
    FUNDAMENTAL_ONLY = "fundamental_only"           # Only fundamental data available
    PRICE_CALCULATED = "price_calculated"           # Calculated from price * shares
    SINGLE_SOURCE = "single_source"                 # Only one data source
    VENDOR_DISAGREEMENT = "vendor_disagreement"     # Sources disagree significantly
    OUTLIER_DETECTED = "outlier_detected"          # Statistical outlier found
    MISSING_DATA = "missing_data"                   # No reliable data available
    MANUAL_REVIEW_REQUIRED = "manual_review_required"


@dataclass
class MarketCapSource:
    """Container for market cap data from a specific source"""
    source_type: str  # 'fundamental', 'price_calculated', 'external'
    vendor: str       # 'fmp', 'polygon', 'tiingo', 'calculated', etc.
    symbol: str
    date: date
    market_cap: int   # Market cap in dollars
    calculation_method: str  # 'reported', 'price_shares', 'consensus'
    price_used: Optional[float] = None
    shares_outstanding: Optional[int] = None
    confidence: float = 1.0
    raw_data: Optional[Dict] = None


@dataclass
class UnifiedMarketCap:
    """Final unified market cap after validation and reconciliation"""
    symbol: str
    date: date
    market_cap: int                                 # Final unified market cap
    confidence_score: float                         # Overall confidence (0.0-1.0)
    status: MarketCapValidationStatus              # Validation status
    primary_source: str                            # Primary data source used
    source_data: List[MarketCapSource]             # All source data used
    validation_metadata: Dict[str, Any]            # Validation details
    calculation_notes: str                          # Human-readable explanation


class UnifiedMarketCapProvider:
    """
    Provider for unified market cap data combining fundamental and price sources
    """

    def __init__(self, environment: Environment):
        self.env = environment
        self.logger = logging.getLogger(__name__)

        # Initialize data providers
        self.fundamental_provider = UnifiedFundamentalProvider(environment)
        self.price_validator = UnifiedDailyPriceValidator(environment)

        # Validation parameters
        self.disagreement_threshold = 0.15  # 15% disagreement threshold
        self.outlier_threshold_sigma = 3.0  # 3-sigma outlier detection
        self.min_confidence_threshold = 0.3  # Minimum confidence for inclusion

        # Database connection
        self.conn: Optional[asyncpg.Connection] = None

    async def connect(self):
        """Establish database connections"""
        if self.conn is None:
            self.conn = await asyncpg.connect(self.env.get_database_url())
            await self.price_validator.connect()
            self.logger.info("✅ Connected to database for market cap calculations")

    async def disconnect(self):
        """Close database connections"""
        if self.conn:
            await self.conn.close()
            self.conn = None
        await self.price_validator.disconnect()

    async def get_unified_market_cap(self, symbol: str, target_date: date) -> Optional[UnifiedMarketCap]:
        """
        Get unified market cap for a symbol on a specific date

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            target_date: Date for market cap calculation

        Returns:
            UnifiedMarketCap object or None if no reliable data available
        """
        try:
            self.logger.info(f"Calculating unified market cap for {symbol} on {target_date}")

            # Step 1: Gather market cap data from all available sources
            market_cap_sources = await self._gather_market_cap_sources(symbol, target_date)

            if not market_cap_sources:
                self.logger.warning(f"No market cap data found for {symbol} on {target_date}")
                return None

            # Step 2: Validate and reconcile data from multiple sources
            unified_market_cap = await self._create_unified_market_cap(symbol, target_date, market_cap_sources)

            if unified_market_cap:
                self.logger.info(f"✅ Unified market cap for {symbol} on {target_date}: "
                               f"${unified_market_cap.market_cap:,} "
                               f"(confidence: {unified_market_cap.confidence_score:.2f}, "
                               f"status: {unified_market_cap.status.value})")

            return unified_market_cap

        except Exception as e:
            self.logger.error(f"Error calculating unified market cap for {symbol} on {target_date}: {e}")
            return None

    async def _gather_market_cap_sources(self, symbol: str, target_date: date) -> List[MarketCapSource]:
        """Gather market cap data from all available sources"""
        sources = []

        # Source 1: Fundamental data (reported market cap)
        fundamental_sources = await self._get_fundamental_market_cap_sources(symbol, target_date)
        sources.extend(fundamental_sources)

        # Source 2: Price-based calculation (price * shares outstanding)
        price_based_source = await self._get_price_based_market_cap(symbol, target_date)
        if price_based_source:
            sources.append(price_based_source)

        # Source 3: Historical market cap calculation (if current data missing)
        if not sources:
            historical_source = await self._get_historical_market_cap_estimate(symbol, target_date)
            if historical_source:
                sources.append(historical_source)

        return sources

    async def _get_fundamental_market_cap_sources(self, symbol: str, target_date: date) -> List[MarketCapSource]:
        """Get market cap from fundamental data sources"""
        sources = []

        try:
            # Get unified fundamental data
            unified_fundamental = await self.fundamental_provider.get_unified_fundamental(symbol, target_date)

            if unified_fundamental and unified_fundamental.vendor_data:
                for vendor_data in unified_fundamental.vendor_data:
                    if vendor_data.market_cap and vendor_data.market_cap > 0:
                        source = MarketCapSource(
                            source_type="fundamental",
                            vendor=vendor_data.vendor,
                            symbol=symbol,
                            date=target_date,
                            market_cap=vendor_data.market_cap,
                            calculation_method="reported",
                            confidence=vendor_data.confidence,
                            raw_data=vendor_data.raw_data
                        )
                        sources.append(source)

        except Exception as e:
            self.logger.warning(f"Error fetching fundamental market cap for {symbol}: {e}")

        return sources

    async def _get_price_based_market_cap(self, symbol: str, target_date: date) -> Optional[MarketCapSource]:
        """Calculate market cap from price * shares outstanding"""
        try:
            # Get unified price data
            unified_price = await self.price_validator.validate_and_unify_price(symbol, target_date)
            if not unified_price or not unified_price.validation_result.is_valid:
                return None

            # Get shares outstanding data
            shares_outstanding = await self._get_shares_outstanding(symbol, target_date)
            if not shares_outstanding or shares_outstanding <= 0:
                return None

            # Calculate market cap
            market_cap = int(unified_price.close * shares_outstanding)

            return MarketCapSource(
                source_type="price_calculated",
                vendor="calculated",
                symbol=symbol,
                date=target_date,
                market_cap=market_cap,
                calculation_method="price_shares",
                price_used=unified_price.close,
                shares_outstanding=shares_outstanding,
                confidence=unified_price.validation_result.confidence_score,
                raw_data={
                    "price": unified_price.close,
                    "shares_outstanding": shares_outstanding,
                    "price_validation": unified_price.validation_result.validation_notes
                }
            )

        except Exception as e:
            self.logger.warning(f"Error calculating price-based market cap for {symbol}: {e}")
            return None

    async def _get_shares_outstanding(self, symbol: str, target_date: date) -> Optional[int]:
        """Get shares outstanding data for market cap calculation"""
        try:
            # Look for shares outstanding in fundamental data first
            unified_fundamental = await self.fundamental_provider.get_unified_fundamental(symbol, target_date)

            # Try to extract shares outstanding from raw fundamental data
            if unified_fundamental and unified_fundamental.vendor_data:
                for vendor_data in unified_fundamental.vendor_data:
                    if vendor_data.raw_data:
                        # Look for common shares outstanding fields
                        shares_fields = [
                            'shares_outstanding', 'common_shares_outstanding',
                            'weighted_average_shares', 'basic_shares_outstanding'
                        ]
                        for field in shares_fields:
                            shares = vendor_data.raw_data.get(field)
                            if shares and isinstance(shares, (int, float)) and shares > 0:
                                return int(shares)

            # Fallback: Query database for shares data
            if self.conn:
                query = """
                    SELECT shares_outstanding
                    FROM dev_company_info
                    WHERE symbol = $1
                    ORDER BY ABS(EXTRACT(days FROM date - $2::date))
                    LIMIT 1
                """
                row = await self.conn.fetchrow(query, symbol, target_date)
                if row and row['shares_outstanding']:
                    return int(row['shares_outstanding'])

            return None

        except Exception as e:
            self.logger.warning(f"Error fetching shares outstanding for {symbol}: {e}")
            return None

    async def _get_historical_market_cap_estimate(self, symbol: str, target_date: date) -> Optional[MarketCapSource]:
        """Get historical market cap estimate when current data is missing"""
        try:
            if not self.conn:
                return None

            # Look for recent market cap data within 30 days
            lookback_start = target_date - timedelta(days=30)

            query = """
                SELECT market_cap, date, 'historical' as source
                FROM dev_fundamentals_comprehensive
                WHERE symbol = $1 AND date BETWEEN $2 AND $3 AND market_cap IS NOT NULL
                ORDER BY ABS(EXTRACT(days FROM date - $4::date))
                LIMIT 1
            """

            row = await self.conn.fetchrow(query, symbol, lookback_start, target_date, target_date)

            if row and row['market_cap']:
                days_difference = abs((row['date'] - target_date).days)
                confidence = max(0.2, 1.0 - (days_difference / 30.0) * 0.6)  # Lower confidence for older data

                return MarketCapSource(
                    source_type="historical",
                    vendor="database",
                    symbol=symbol,
                    date=target_date,
                    market_cap=int(row['market_cap']),
                    calculation_method="historical_estimate",
                    confidence=confidence,
                    raw_data={
                        "original_date": row['date'],
                        "days_difference": days_difference,
                        "source": "historical_lookup"
                    }
                )

            return None

        except Exception as e:
            self.logger.warning(f"Error fetching historical market cap for {symbol}: {e}")
            return None

    async def _create_unified_market_cap(self, symbol: str, target_date: date,
                                       sources: List[MarketCapSource]) -> Optional[UnifiedMarketCap]:
        """Create unified market cap from multiple sources with validation"""
        if not sources:
            return None

        # Filter sources by minimum confidence
        valid_sources = [s for s in sources if s.confidence >= self.min_confidence_threshold]
        if not valid_sources:
            valid_sources = sources  # Use all sources if none meet confidence threshold

        # Single source case
        if len(valid_sources) == 1:
            source = valid_sources[0]
            return UnifiedMarketCap(
                symbol=symbol,
                date=target_date,
                market_cap=source.market_cap,
                confidence_score=source.confidence * 0.7,  # Lower confidence for single source
                status=MarketCapValidationStatus.SINGLE_SOURCE,
                primary_source=f"{source.source_type}_{source.vendor}",
                source_data=valid_sources,
                validation_metadata={"single_source": True},
                calculation_notes=f"Single source: {source.calculation_method} from {source.vendor}"
            )

        # Multiple sources - perform validation and reconciliation
        market_caps = [s.market_cap for s in valid_sources]

        # Statistical analysis
        mean_cap = statistics.mean(market_caps)
        median_cap = statistics.median(market_caps)
        std_cap = statistics.stdev(market_caps) if len(market_caps) > 1 else 0

        # Detect outliers
        outliers = []
        if std_cap > 0:
            for i, cap in enumerate(market_caps):
                z_score = abs((cap - mean_cap) / std_cap)
                if z_score > self.outlier_threshold_sigma:
                    outliers.append(valid_sources[i])

        # Remove outliers for consensus calculation
        consensus_sources = [s for s in valid_sources if s not in outliers]
        if not consensus_sources:
            consensus_sources = valid_sources  # Keep all if no consensus

        # Calculate disagreement
        consensus_caps = [s.market_cap for s in consensus_sources]
        max_deviation = max(abs(cap - mean_cap) for cap in consensus_caps)
        max_deviation_pct = (max_deviation / mean_cap) if mean_cap > 0 else 0

        # Determine status and confidence
        if max_deviation_pct > self.disagreement_threshold:
            status = MarketCapValidationStatus.VENDOR_DISAGREEMENT
            confidence = 0.4
        elif len(outliers) > 0:
            status = MarketCapValidationStatus.OUTLIER_DETECTED
            confidence = 0.6
        elif len(consensus_sources) == len(valid_sources):
            status = MarketCapValidationStatus.CONSENSUS
            confidence = min(0.95, 0.7 + (len(consensus_sources) * 0.05))
        else:
            status = MarketCapValidationStatus.MAJORITY_CONSENSUS
            confidence = 0.8

        # Final market cap calculation (weighted by confidence)
        if consensus_sources:
            total_weight = sum(s.confidence for s in consensus_sources)
            if total_weight > 0:
                weighted_market_cap = sum(s.market_cap * s.confidence for s in consensus_sources) / total_weight
                final_market_cap = int(weighted_market_cap)
            else:
                final_market_cap = int(statistics.median([s.market_cap for s in consensus_sources]))
        else:
            final_market_cap = int(median_cap)

        # Select primary source (highest confidence)
        primary_source = max(consensus_sources, key=lambda s: s.confidence)

        # Validation metadata
        validation_metadata = {
            "total_sources": len(sources),
            "valid_sources": len(valid_sources),
            "consensus_sources": len(consensus_sources),
            "outliers_detected": len(outliers),
            "mean_market_cap": int(mean_cap),
            "median_market_cap": int(median_cap),
            "std_deviation": int(std_cap),
            "max_deviation_pct": max_deviation_pct,
            "disagreement_threshold": self.disagreement_threshold,
            "source_breakdown": {s.vendor: s.market_cap for s in valid_sources}
        }

        # Calculation notes
        calculation_notes = (
            f"Consensus from {len(consensus_sources)} sources "
            f"(max deviation: {max_deviation_pct:.1%}). "
            f"Primary: {primary_source.vendor} ({primary_source.calculation_method})"
        )

        return UnifiedMarketCap(
            symbol=symbol,
            date=target_date,
            market_cap=final_market_cap,
            confidence_score=confidence,
            status=status,
            primary_source=f"{primary_source.source_type}_{primary_source.vendor}",
            source_data=valid_sources,
            validation_metadata=validation_metadata,
            calculation_notes=calculation_notes
        )

    async def list_symbols_with_market_cap_data(self, start_date: Optional[date] = None,
                                               end_date: Optional[date] = None) -> List[str]:
        """List symbols that have market cap data available"""
        try:
            # Get symbols from both fundamental and price data
            fundamental_symbols = await self.fundamental_provider.list_symbols_with_data(start_date, end_date)

            # Also check for symbols with price data (for price-based calculation)
            price_symbols = []
            if self.conn:
                query = """
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    JOIN dev_daily_price_polygon p ON i.id = p.instrument_id
                    WHERE ($1::date IS NULL OR p.date >= $1)
                      AND ($2::date IS NULL OR p.date <= $2)
                    UNION
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    JOIN dev_daily_price_tiingo p ON i.id = p.instrument_id
                    WHERE ($1::date IS NULL OR p.date >= $1)
                      AND ($2::date IS NULL OR p.date <= $2)
                    ORDER BY symbol
                """
                rows = await self.conn.fetch(query, start_date, end_date)
                price_symbols = [row['symbol'] for row in rows]

            # Return union of both symbol lists
            all_symbols = list(set(fundamental_symbols + price_symbols))
            all_symbols.sort()
            return all_symbols

        except Exception as e:
            self.logger.error(f"Error listing symbols with market cap data: {e}")
            return []

    async def get_market_cap_history(self, symbol: str, start_date: date,
                                   end_date: date) -> List[UnifiedMarketCap]:
        """Get market cap history for a symbol over a date range"""
        try:
            history = []
            current_date = start_date

            while current_date <= end_date:
                market_cap = await self.get_unified_market_cap(symbol, current_date)
                if market_cap:
                    history.append(market_cap)

                current_date += timedelta(days=1)

            return history

        except Exception as e:
            self.logger.error(f"Error getting market cap history for {symbol}: {e}")
            return []