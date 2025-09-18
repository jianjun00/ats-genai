"""
Unified Fundamental Provider

Combines fundamental data from multiple vendors (FMP, Polygon, Tiingo) with
cross-vendor validation, outlier detection, and confidence scoring similar
to the UnifiedPrices architecture.

This provider:
1. Aggregates fundamental data from all available vendors
2. Performs cross-vendor validation and outlier detection
3. Provides confidence scoring based on vendor agreement
4. Handles missing data gracefully
5. Maintains audit trails and metadata
"""

from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import date, datetime
from dataclasses import dataclass, field
from enum import Enum
import logging
import statistics

from src.core.shared.utils.environment import Environment
from vendor.fmp.dao.fundamentals_fmp_dao import FundamentalsFMPDAO, FMPFundamental
from vendor.polygon.dao.fundamentals_polygon_dao import FundamentalsPolygonDAO, PolygonFundamental
from vendor.tiingo.dao.fundamentals_tiingo_dao import FundamentalsTiingoDAO, TiingoFundamental


class ValidationStatus(Enum):
    """Validation status for unified fundamental data"""
    VALID = "valid"
    SINGLE_SOURCE = "single_source"
    VENDOR_DISAGREEMENT = "vendor_disagreement"
    OUTLIER_STATISTICAL = "outlier_statistical"
    MISSING_CRITICAL_DATA = "missing_critical_data"
    DATA_QUALITY_ISSUE = "data_quality_issue"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"


@dataclass
class VendorFundamental:
    """Container for fundamental data from a specific vendor"""
    vendor: str
    symbol: str
    date: date
    fiscal_period: Optional[str] = None
    revenue: Optional[int] = None
    gross_profit: Optional[int] = None
    operating_income: Optional[int] = None
    net_income: Optional[int] = None
    ebitda: Optional[int] = None
    eps: Optional[float] = None
    total_assets: Optional[int] = None
    total_liabilities: Optional[int] = None
    shareholders_equity: Optional[int] = None
    current_assets: Optional[int] = None
    current_liabilities: Optional[int] = None
    total_debt: Optional[int] = None
    cash_and_equivalents: Optional[int] = None
    operating_cash_flow: Optional[int] = None
    investing_cash_flow: Optional[int] = None
    financing_cash_flow: Optional[int] = None
    free_cash_flow: Optional[int] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    confidence: float = 1.0
    raw_data: Optional[Dict] = None

    def get_numeric_fields(self) -> Dict[str, Union[int, float]]:
        """Get all numeric fields for comparison"""
        numeric_fields = {}

        # Integer fields
        int_fields = [
            'revenue', 'gross_profit', 'operating_income', 'net_income', 'ebitda',
            'total_assets', 'total_liabilities', 'shareholders_equity', 'current_assets',
            'current_liabilities', 'total_debt', 'cash_and_equivalents',
            'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow',
            'free_cash_flow', 'market_cap'
        ]

        # Float fields
        float_fields = [
            'eps', 'pe_ratio', 'pb_ratio', 'debt_to_equity', 'roe', 'roa',
            'current_ratio', 'quick_ratio'
        ]

        for field in int_fields + float_fields:
            value = getattr(self, field)
            if value is not None:
                numeric_fields[field] = value

        return numeric_fields


@dataclass
class UnifiedFundamental:
    """Unified fundamental data with validation metadata"""
    symbol: str
    date: date
    fiscal_period: Optional[str] = None

    # Core financial metrics (unified values)
    revenue: Optional[int] = None
    gross_profit: Optional[int] = None
    operating_income: Optional[int] = None
    net_income: Optional[int] = None
    ebitda: Optional[int] = None
    eps: Optional[float] = None
    total_assets: Optional[int] = None
    total_liabilities: Optional[int] = None
    shareholders_equity: Optional[int] = None
    current_assets: Optional[int] = None
    current_liabilities: Optional[int] = None
    total_debt: Optional[int] = None
    cash_and_equivalents: Optional[int] = None
    operating_cash_flow: Optional[int] = None
    investing_cash_flow: Optional[int] = None
    financing_cash_flow: Optional[int] = None
    free_cash_flow: Optional[int] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None

    # Validation metadata
    validation_status: ValidationStatus = ValidationStatus.VALID
    confidence_score: float = 1.0
    vendor_count: int = 0
    vendor_sources: List[str] = field(default_factory=list)
    disagreement_fields: List[str] = field(default_factory=list)
    outlier_fields: List[str] = field(default_factory=list)
    validation_notes: str = ""

    # Raw vendor data for audit
    vendor_data: Dict[str, Dict] = field(default_factory=dict)

    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class UnifiedFundamentalProvider:
    """Unified fundamental data provider with cross-vendor validation"""

    def __init__(self, env: Environment):
        self.env = env
        self.logger = logging.getLogger(__name__)

        # Initialize vendor DAOs
        self.fmp_dao = FundamentalsFMPDAO(env)
        self.polygon_dao = FundamentalsPolygonDAO(env)
        self.tiingo_dao = FundamentalsTiingoDAO(env)

        # Validation thresholds
        self.disagreement_threshold = 0.15  # 15% disagreement threshold
        self.outlier_z_score_threshold = 2.5  # Z-score threshold for outliers
        self.minimum_confidence_score = 0.6

    async def get_unified_fundamental(self, symbol: str, date: date) -> Optional[UnifiedFundamental]:
        """Get unified fundamental data for a symbol and date"""
        self.logger.debug(f"Getting unified fundamental for {symbol} on {date}")

        try:
            # Fetch data from all vendors
            vendor_fundamentals = await self._fetch_vendor_data(symbol, date)

            if not vendor_fundamentals:
                self.logger.warning(f"No fundamental data found for {symbol} on {date}")
                return None

            # Create unified fundamental
            unified = await self._create_unified_fundamental(vendor_fundamentals)

            self.logger.info(f"Created unified fundamental for {symbol} with {len(vendor_fundamentals)} vendors")
            return unified

        except Exception as e:
            self.logger.error(f"Error getting unified fundamental for {symbol}: {e}")
            return None

    async def get_unified_fundamentals(self, symbol: str, start_date: Optional[date] = None,
                                     end_date: Optional[date] = None, limit: int = 50) -> List[UnifiedFundamental]:
        """Get unified fundamental data for a symbol over a date range"""
        self.logger.debug(f"Getting unified fundamentals for {symbol} from {start_date} to {end_date}")

        try:
            # Get all dates that have fundamental data across vendors
            dates_with_data = await self._get_dates_with_data(symbol, start_date, end_date, limit)

            if not dates_with_data:
                return []

            # Get unified fundamentals for each date
            unified_fundamentals = []
            for date_entry in dates_with_data:
                unified = await self.get_unified_fundamental(symbol, date_entry)
                if unified:
                    unified_fundamentals.append(unified)

            return sorted(unified_fundamentals, key=lambda x: x.date, reverse=True)

        except Exception as e:
            self.logger.error(f"Error getting unified fundamentals for {symbol}: {e}")
            return []

    async def _fetch_vendor_data(self, symbol: str, date: date) -> List[VendorFundamental]:
        """Fetch fundamental data from all vendors for a symbol and date"""
        vendor_data = []

        # Fetch from FMP
        try:
            fmp_data = await self.fmp_dao.get_fundamental(symbol, date)
            if fmp_data:
                vendor_data.append(self._convert_to_vendor_fundamental(fmp_data))
        except Exception as e:
            self.logger.warning(f"Error fetching FMP data for {symbol}: {e}")

        # Fetch from Polygon
        try:
            polygon_data = await self.polygon_dao.get_fundamental(symbol, date)
            if polygon_data:
                vendor_data.append(self._convert_to_vendor_fundamental(polygon_data))
        except Exception as e:
            self.logger.warning(f"Error fetching Polygon data for {symbol}: {e}")

        # Fetch from Tiingo
        try:
            tiingo_data = await self.tiingo_dao.get_fundamental(symbol, date)
            if tiingo_data:
                vendor_data.append(self._convert_to_vendor_fundamental(tiingo_data))
        except Exception as e:
            self.logger.warning(f"Error fetching Tiingo data for {symbol}: {e}")

        return vendor_data

    def _convert_to_vendor_fundamental(self, data: Union[FMPFundamental, PolygonFundamental, TiingoFundamental]) -> VendorFundamental:
        """Convert vendor-specific fundamental to generic VendorFundamental"""
        return VendorFundamental(
            vendor=data.vendor,
            symbol=data.symbol,
            date=data.date,
            fiscal_period=data.fiscal_period,
            revenue=data.revenue,
            gross_profit=data.gross_profit,
            operating_income=data.operating_income,
            net_income=data.net_income,
            ebitda=data.ebitda,
            eps=data.eps,
            total_assets=data.total_assets,
            total_liabilities=data.total_liabilities,
            shareholders_equity=data.shareholders_equity,
            current_assets=data.current_assets,
            current_liabilities=data.current_liabilities,
            total_debt=data.total_debt,
            cash_and_equivalents=data.cash_and_equivalents,
            operating_cash_flow=data.operating_cash_flow,
            investing_cash_flow=data.investing_cash_flow,
            financing_cash_flow=data.financing_cash_flow,
            free_cash_flow=data.free_cash_flow,
            market_cap=data.market_cap,
            pe_ratio=data.pe_ratio,
            pb_ratio=data.pb_ratio,
            debt_to_equity=data.debt_to_equity,
            roe=data.roe,
            roa=data.roa,
            current_ratio=data.current_ratio,
            quick_ratio=data.quick_ratio,
            raw_data=data.raw_data
        )

    async def _create_unified_fundamental(self, vendor_fundamentals: List[VendorFundamental]) -> UnifiedFundamental:
        """Create unified fundamental from vendor data with validation"""
        if not vendor_fundamentals:
            raise ValueError("No vendor data provided")

        base_data = vendor_fundamentals[0]
        unified = UnifiedFundamental(
            symbol=base_data.symbol,
            date=base_data.date,
            vendor_count=len(vendor_fundamentals),
            vendor_sources=[vf.vendor for vf in vendor_fundamentals]
        )

        # Store raw vendor data for audit
        for vf in vendor_fundamentals:
            unified.vendor_data[vf.vendor] = vf.__dict__.copy()

        # If only one vendor, use its data directly
        if len(vendor_fundamentals) == 1:
            unified.validation_status = ValidationStatus.SINGLE_SOURCE
            unified.confidence_score = 0.7  # Lower confidence for single source
            self._copy_fundamental_values(base_data, unified)
            return unified

        # Perform cross-vendor validation and unification
        await self._unify_fundamental_fields(vendor_fundamentals, unified)

        return unified

    def _copy_fundamental_values(self, source: VendorFundamental, target: UnifiedFundamental):
        """Copy fundamental values from source to target"""
        fields_to_copy = [
            'fiscal_period', 'revenue', 'gross_profit', 'operating_income', 'net_income',
            'ebitda', 'eps', 'total_assets', 'total_liabilities', 'shareholders_equity',
            'current_assets', 'current_liabilities', 'total_debt', 'cash_and_equivalents',
            'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow',
            'free_cash_flow', 'market_cap', 'pe_ratio', 'pb_ratio', 'debt_to_equity',
            'roe', 'roa', 'current_ratio', 'quick_ratio'
        ]

        for field in fields_to_copy:
            if hasattr(source, field) and hasattr(target, field):
                setattr(target, field, getattr(source, field))

    async def _unify_fundamental_fields(self, vendor_fundamentals: List[VendorFundamental], unified: UnifiedFundamental):
        """Unify fundamental fields across vendors with validation"""
        disagreement_fields = []
        outlier_fields = []

        # Get all numeric fields for comparison
        all_fields = set()
        for vf in vendor_fundamentals:
            all_fields.update(vf.get_numeric_fields().keys())

        # Unify each field
        for field in all_fields:
            values = []
            vendors_with_field = []

            # Collect values from all vendors
            for vf in vendor_fundamentals:
                field_data = vf.get_numeric_fields()
                if field in field_data and field_data[field] is not None:
                    values.append(field_data[field])
                    vendors_with_field.append(vf.vendor)

            if not values:
                continue

            # Unify the field value
            unified_value, field_confidence, has_disagreement, is_outlier = self._unify_field_values(values, vendors_with_field)

            # Set the unified value
            if hasattr(unified, field):
                setattr(unified, field, unified_value)

            # Track disagreements and outliers
            if has_disagreement:
                disagreement_fields.append(field)
            if is_outlier:
                outlier_fields.append(field)

        # Handle fiscal period (string field)
        fiscal_periods = [vf.fiscal_period for vf in vendor_fundamentals if vf.fiscal_period]
        if fiscal_periods:
            # Use most common fiscal period
            unified.fiscal_period = max(set(fiscal_periods), key=fiscal_periods.count)

        # Calculate overall confidence and validation status
        unified.disagreement_fields = disagreement_fields
        unified.outlier_fields = outlier_fields

        # Determine validation status
        if outlier_fields:
            unified.validation_status = ValidationStatus.OUTLIER_STATISTICAL
            unified.confidence_score = max(0.4, 0.8 - len(outlier_fields) * 0.1)
        elif disagreement_fields:
            unified.validation_status = ValidationStatus.VENDOR_DISAGREEMENT
            unified.confidence_score = max(0.5, 0.9 - len(disagreement_fields) * 0.1)
        else:
            unified.validation_status = ValidationStatus.VALID
            unified.confidence_score = min(1.0, 0.8 + unified.vendor_count * 0.1)

        # Add validation notes
        notes = []
        if disagreement_fields:
            notes.append(f"Vendor disagreement in: {', '.join(disagreement_fields[:3])}")
        if outlier_fields:
            notes.append(f"Statistical outliers in: {', '.join(outlier_fields[:3])}")
        if unified.vendor_count == 1:
            notes.append("Single vendor source")

        unified.validation_notes = "; ".join(notes) if notes else "Cross-vendor validation passed"

    def _unify_field_values(self, values: List[Union[int, float]], vendors: List[str]) -> Tuple[Union[int, float], float, bool, bool]:
        """Unify field values with disagreement and outlier detection"""
        if not values:
            return None, 0.0, False, False

        if len(values) == 1:
            return values[0], 0.7, False, False

        # Calculate statistics
        mean_value = statistics.mean(values)

        # Check for outliers using z-score
        has_outliers = False
        if len(values) > 2:
            try:
                stdev = statistics.stdev(values)
                if stdev > 0:
                    z_scores = [abs(v - mean_value) / stdev for v in values]
                    has_outliers = any(z > self.outlier_z_score_threshold for z in z_scores)
            except:
                pass

        # Check for significant disagreement
        has_disagreement = False
        if len(values) > 1:
            try:
                max_val = max(values)
                min_val = min(values)
                if max_val > 0:
                    disagreement_ratio = abs(max_val - min_val) / max_val
                    has_disagreement = disagreement_ratio > self.disagreement_threshold
            except:
                pass

        # Choose unified value (median for better outlier resistance)
        if len(values) >= 3:
            unified_value = statistics.median(values)
        else:
            unified_value = mean_value

        # Calculate field confidence
        confidence = 1.0
        if has_outliers:
            confidence -= 0.3
        if has_disagreement:
            confidence -= 0.2

        # Convert back to appropriate type
        if isinstance(values[0], int):
            unified_value = int(round(unified_value))

        return unified_value, max(0.1, confidence), has_disagreement, has_outliers

    async def _get_dates_with_data(self, symbol: str, start_date: Optional[date],
                                  end_date: Optional[date], limit: int) -> List[date]:
        """Get dates that have fundamental data for the symbol"""
        # This could be optimized by querying the database directly
        # For now, we'll get dates from each vendor and combine them

        all_dates = set()

        # Get dates from FMP
        try:
            fmp_fundamentals = await self.fmp_dao.list_fundamentals(symbol, start_date, end_date, limit)
            all_dates.update(f.date for f in fmp_fundamentals)
        except Exception as e:
            self.logger.warning(f"Error getting FMP dates for {symbol}: {e}")

        # Get dates from Polygon
        try:
            polygon_fundamentals = await self.polygon_dao.list_fundamentals(symbol, start_date, end_date, limit)
            all_dates.update(f.date for f in polygon_fundamentals)
        except Exception as e:
            self.logger.warning(f"Error getting Polygon dates for {symbol}: {e}")

        # Get dates from Tiingo
        try:
            tiingo_fundamentals = await self.tiingo_dao.list_fundamentals(symbol, start_date, end_date, limit)
            all_dates.update(f.date for f in tiingo_fundamentals)
        except Exception as e:
            self.logger.warning(f"Error getting Tiingo dates for {symbol}: {e}")

        # Sort and limit
        sorted_dates = sorted(all_dates, reverse=True)
        return sorted_dates[:limit] if limit else sorted_dates

    async def get_vendor_comparison(self, symbol: str, date: date) -> Optional[Dict[str, Any]]:
        """Get detailed vendor comparison for debugging and analysis"""
        vendor_fundamentals = await self._fetch_vendor_data(symbol, date)

        if not vendor_fundamentals:
            return None

        comparison = {
            'symbol': symbol,
            'date': date.isoformat(),
            'vendor_count': len(vendor_fundamentals),
            'vendors': {},
            'field_comparison': {}
        }

        # Add vendor data
        for vf in vendor_fundamentals:
            comparison['vendors'][vf.vendor] = vf.__dict__.copy()
            # Convert date to string for JSON serialization
            comparison['vendors'][vf.vendor]['date'] = vf.date.isoformat()

        # Compare fields across vendors
        all_fields = set()
        for vf in vendor_fundamentals:
            all_fields.update(vf.get_numeric_fields().keys())

        for field in all_fields:
            field_values = {}
            for vf in vendor_fundamentals:
                field_data = vf.get_numeric_fields()
                if field in field_data:
                    field_values[vf.vendor] = field_data[field]

            if len(field_values) > 1:
                values = list(field_values.values())
                try:
                    comparison['field_comparison'][field] = {
                        'values': field_values,
                        'mean': statistics.mean(values),
                        'median': statistics.median(values),
                        'min': min(values),
                        'max': max(values),
                        'disagreement_ratio': abs(max(values) - min(values)) / max(values) if max(values) > 0 else 0
                    }
                except:
                    comparison['field_comparison'][field] = {
                        'values': field_values,
                        'error': 'Unable to calculate statistics'
                    }

        return comparison