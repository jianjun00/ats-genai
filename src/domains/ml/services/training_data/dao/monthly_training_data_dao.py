#!/usr/bin/env python3
"""
Monthly Training Data DAO

Data Access Object for monthly training data tracking with timeframe file paths.
Provides granular record-level access for EDA navigation and visualization.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import date, datetime
from dataclasses import dataclass
import asyncpg

from core.shared.utils.environment import Environment

logger = logging.getLogger(__name__)


@dataclass
class MonthlyTrainingDataRecord:
    """Record representing monthly training data with timeframe paths."""

    # Primary identifiers
    run_id: int
    symbol: str
    instrument_id: Optional[int]
    year_month: date  # First day of the month

    # File paths for different timeframes
    timeframe_paths: Dict[str, str]  # e.g., {"5m": "/path/to/5m.arrayrecord", "15m": "/path/to/15m.arrayrecord"}

    # Metadata
    total_records: int = 0
    file_size_mb: float = 0.0
    data_quality_score: float = 0.0

    # Status tracking
    status: str = "created"  # created, processing, completed, failed
    error_message: str = ""

    # Database fields (auto-populated)
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Extended fields from join with instruments table
    instrument_name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    market_cap: Optional[float] = None


class MonthlyTrainingDataDAO:
    """Data Access Object for monthly training data tracking."""

    def __init__(self, environment: Environment):
        """Initialize DAO with environment configuration."""
        self.environment = environment
        self.table_name = environment.get_table_name("monthly_training_data")
        self.view_name = f"{self.table_name}_with_instruments"

    async def create_monthly_record(self, record: MonthlyTrainingDataRecord) -> int:
        """Create a new monthly training data record."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            query = f"""
            INSERT INTO {self.table_name} (
                run_id, symbol, instrument_id, year_month, timeframe_paths,
                total_records, file_size_mb, data_quality_score, status, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            """

            record_id = await conn.fetchval(
                query,
                record.run_id,
                record.symbol,
                record.instrument_id,
                record.year_month,
                json.dumps(record.timeframe_paths),
                record.total_records,
                record.file_size_mb,
                record.data_quality_score,
                record.status,
                record.error_message
            )

            logger.info(f"✅ Created monthly training data record ID: {record_id}")
            logger.info(f"   Symbol: {record.symbol}, Month: {record.year_month}, Run: {record.run_id}")

            return record_id

        finally:
            await conn.close()

    async def update_monthly_record(self, record_id: int, updates: Dict[str, Any]) -> bool:
        """Update a monthly training data record."""

        if not updates:
            logger.warning("No updates provided for monthly training data record")
            return False

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            # Build dynamic update query
            set_clauses = []
            values = []
            param_index = 1

            for key, value in updates.items():
                if key == 'timeframe_paths' and isinstance(value, dict):
                    set_clauses.append(f"{key} = ${param_index}")
                    values.append(json.dumps(value))
                else:
                    set_clauses.append(f"{key} = ${param_index}")
                    values.append(value)
                param_index += 1

            # Add updated_at
            set_clauses.append(f"updated_at = ${param_index}")
            values.append(datetime.now())
            param_index += 1

            # Add WHERE clause
            values.append(record_id)

            query = f"""
            UPDATE {self.table_name}
            SET {', '.join(set_clauses)}
            WHERE id = ${param_index}
            """

            result = await conn.execute(query, *values)
            success = result == "UPDATE 1"

            if success:
                logger.info(f"✅ Updated monthly training data record ID: {record_id}")
                logger.debug(f"   Updates: {updates}")
            else:
                logger.warning(f"⚠️ No record found to update with ID: {record_id}")

            return success

        finally:
            await conn.close()

    async def get_monthly_record(self, record_id: int, include_instrument_details: bool = True) -> Optional[MonthlyTrainingDataRecord]:
        """Get a single monthly training data record by ID."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            if include_instrument_details:
                query = f"SELECT * FROM {self.view_name} WHERE id = $1"
            else:
                query = f"SELECT * FROM {self.table_name} WHERE id = $1"

            row = await conn.fetchrow(query, record_id)

            if not row:
                logger.warning(f"No monthly training data record found with ID: {record_id}")
                return None

            return self._row_to_record(row)

        finally:
            await conn.close()

    async def list_monthly_records(
        self,
        symbols: Optional[List[str]] = None,
        year_months: Optional[List[date]] = None,
        run_ids: Optional[List[int]] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "DESC",
        include_instrument_details: bool = True
    ) -> List[MonthlyTrainingDataRecord]:
        """List monthly training data records with filtering and pagination."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            # Build WHERE clauses
            where_clauses = []
            params = []
            param_index = 1

            if symbols:
                where_clauses.append(f"symbol = ANY(${param_index})")
                params.append(symbols)
                param_index += 1

            if year_months:
                where_clauses.append(f"year_month = ANY(${param_index})")
                params.append(year_months)
                param_index += 1

            if run_ids:
                where_clauses.append(f"run_id = ANY(${param_index})")
                params.append(run_ids)
                param_index += 1

            if status:
                where_clauses.append(f"status = ${param_index}")
                params.append(status)
                param_index += 1

            # Build query
            table_or_view = self.view_name if include_instrument_details else self.table_name
            base_query = f"SELECT * FROM {table_or_view}"

            if where_clauses:
                base_query += f" WHERE {' AND '.join(where_clauses)}"

            # Add ordering
            valid_order_columns = ["id", "symbol", "year_month", "created_at", "total_records", "data_quality_score"]
            if order_by not in valid_order_columns:
                order_by = "created_at"

            order_direction = "DESC" if order_direction.upper() == "DESC" else "ASC"
            base_query += f" ORDER BY {order_by} {order_direction}"

            # Add pagination
            base_query += f" LIMIT ${param_index} OFFSET ${param_index + 1}"
            params.extend([limit, offset])

            rows = await conn.fetch(base_query, *params)

            records = [self._row_to_record(row) for row in rows]

            logger.debug(f"Retrieved {len(records)} monthly training data records")
            return records

        finally:
            await conn.close()

    async def get_records_for_symbol_and_month(
        self,
        symbol: str,
        year_month: date,
        include_instrument_details: bool = True
    ) -> List[MonthlyTrainingDataRecord]:
        """Get all records for a specific symbol and month (may be multiple runs)."""

        return await self.list_monthly_records(
            symbols=[symbol],
            year_months=[year_month],
            include_instrument_details=include_instrument_details
        )

    async def get_records_for_run(self, run_id: int) -> List[MonthlyTrainingDataRecord]:
        """Get all monthly records for a specific run."""

        return await self.list_monthly_records(run_ids=[run_id])

    async def get_timeframe_paths(self, symbol: str, year_month: date, run_id: Optional[int] = None) -> Optional[Dict[str, str]]:
        """Get timeframe file paths for a specific symbol and month."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            query = f"""
            SELECT timeframe_paths
            FROM {self.table_name}
            WHERE symbol = $1 AND year_month = $2
            """
            params = [symbol, year_month]

            if run_id:
                query += " AND run_id = $3"
                params.append(run_id)

            # If multiple records, get the most recent
            query += " ORDER BY created_at DESC LIMIT 1"

            row = await conn.fetchrow(query, *params)

            if not row or not row['timeframe_paths']:
                logger.warning(f"No timeframe paths found for {symbol} {year_month}")
                return None

            return json.loads(row['timeframe_paths'])

        finally:
            await conn.close()

    async def get_summary_by_symbol(self) -> List[Dict[str, Any]]:
        """Get summary statistics grouped by symbol for EDA overview."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            query = f"""
            SELECT
                symbol,
                COUNT(*) as total_months,
                MIN(year_month) as earliest_month,
                MAX(year_month) as latest_month,
                SUM(total_records) as total_records_all_months,
                AVG(data_quality_score) as avg_quality_score,
                SUM(file_size_mb) as total_size_mb,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_months,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_months
            FROM {self.table_name}
            GROUP BY symbol
            ORDER BY total_records_all_months DESC
            """

            rows = await conn.fetch(query)

            summaries = []
            for row in rows:
                summaries.append({
                    'symbol': row['symbol'],
                    'total_months': row['total_months'],
                    'earliest_month': row['earliest_month'],
                    'latest_month': row['latest_month'],
                    'total_records': row['total_records_all_months'],
                    'avg_quality_score': float(row['avg_quality_score']) if row['avg_quality_score'] else 0.0,
                    'total_size_mb': float(row['total_size_mb']) if row['total_size_mb'] else 0.0,
                    'completed_months': row['completed_months'],
                    'failed_months': row['failed_months']
                })

            return summaries

        finally:
            await conn.close()

    async def delete_monthly_record(self, record_id: int) -> bool:
        """Delete a monthly training data record."""

        conn = await asyncpg.connect(self.environment.get_database_url())
        try:
            query = f"DELETE FROM {self.table_name} WHERE id = $1"
            result = await conn.execute(query, record_id)

            success = result == "DELETE 1"
            if success:
                logger.info(f"✅ Deleted monthly training data record ID: {record_id}")
            else:
                logger.warning(f"⚠️ No record found to delete with ID: {record_id}")

            return success

        finally:
            await conn.close()

    def _row_to_record(self, row) -> MonthlyTrainingDataRecord:
        """Convert database row to MonthlyTrainingDataRecord object."""

        # Parse timeframe_paths JSON
        timeframe_paths = {}
        if row['timeframe_paths']:
            try:
                timeframe_paths = json.loads(row['timeframe_paths'])
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse timeframe_paths JSON: {e}")
                timeframe_paths = {}

        return MonthlyTrainingDataRecord(
            id=row['id'],
            run_id=row['run_id'],
            symbol=row['symbol'],
            instrument_id=row['instrument_id'],
            year_month=row['year_month'],
            timeframe_paths=timeframe_paths,
            total_records=row['total_records'] or 0,
            file_size_mb=float(row['file_size_mb']) if row['file_size_mb'] else 0.0,
            data_quality_score=float(row['data_quality_score']) if row['data_quality_score'] else 0.0,
            status=row['status'] or "created",
            error_message=row['error_message'] or "",
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            # Extended fields (only present when using view)
            instrument_name=row.get('instrument_name'),
            exchange=row.get('exchange'),
            sector=row.get('sector'),
            market_cap=float(row['market_cap']) if row.get('market_cap') else None
        )