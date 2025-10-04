#!/usr/bin/env python3
"""
Feature Extraction DAO - Database Access Objects for Feature Extraction System

This module provides database access for the new feature extraction architecture
that replaces the training data generation system. It handles:

- Feature extraction runs tracking
- Feature availability registration 
- Feature group management
- Quality metrics monitoring
- Production tag management
"""

import asyncio
import logging
import re
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field

from core.platform.config.environment import Environment, EnvironmentType


@dataclass
class FeatureExtractionRun:
    """Feature extraction run record matching dev_feature_extraction_runs schema."""
    
    id: Optional[int] = None
    run_id: str = ""
    run_type: str = "feature_extraction"
    status: str = "running"
    feature_groups: List[str] = field(default_factory=list)
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    total_instruments: int = 0
    total_features_generated: int = 0
    execution_duration_seconds: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    command_line: Optional[str] = None
    git_commit_hash: Optional[str] = None
    environment: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


@dataclass
class FeatureExtractionInstrument:
    """Feature extraction instrument record matching dev_feature_extraction_instruments schema."""
    
    id: Optional[int] = None
    run_id: int = 0
    instrument_id: int = 0
    symbol: str = ""
    status: str = "pending"
    features_generated: int = 0
    processing_duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class FeatureAvailability:
    """Feature availability record matching dev_feature_availability schema."""
    
    id: Optional[int] = None
    feature_group_id: int = 0
    instrument_id: int = 0
    symbol: str = ""
    year_month: date = None
    file_path: str = ""
    file_size_bytes: Optional[int] = None
    record_count: Optional[int] = None
    date_range_start: Optional[datetime] = None
    date_range_end: Optional[datetime] = None
    quality_score: Optional[float] = None
    validation_status: str = "pending"
    validation_errors: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class FeatureGroup:
    """Feature group record matching dev_feature_groups schema."""
    
    id: Optional[int] = None
    group_name: str = ""
    display_name: str = ""
    description: Optional[str] = None
    category: str = ""
    update_frequency: str = ""
    computation_lag_minutes: int = 0
    dependencies: List[str] = field(default_factory=list)
    storage_format: str = "arrayrecord"
    retention_months: int = 60
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class FeatureCatalog:
    """Feature catalog record matching dev_feature_catalog schema."""
    
    feature_id: Optional[int] = None
    feature_name: str = ""
    feature_group_id: int = 0
    data_type: str = "FLOAT64"
    column_position: int = 0
    description: Optional[str] = None
    computation_method: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    validation_rules: Optional[Dict[str, Any]] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class FeaturePattern:
    """Feature pattern record matching dev_feature_patterns schema."""
    
    id: Optional[int] = None
    pattern: str = ""
    feature_group_id: int = 0
    pattern_type: str = "contains"
    priority: int = 100
    description: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class FeatureMappingResult:
    """Result of feature name to feature group mapping."""
    
    feature_name: str = ""
    feature_group_name: str = ""
    feature_group_id: int = 0
    match_type: str = "unknown"  # exact, pattern, default
    pattern_matched: Optional[str] = None
    confidence: float = 1.0


class FeatureExtractionDAO:
    """Data Access Object for feature extraction system."""

    def __init__(self, environment: Environment):
        """Initialize DAO with environment."""
        self.environment = environment
        self.logger = logging.getLogger(__name__)
        self.table_prefix = environment.env_type.value
        
        # Cache for performance optimization
        self._feature_groups_cache: Optional[Dict[int, FeatureGroup]] = None
        self._feature_catalog_cache: Optional[Dict[str, FeatureCatalog]] = None
        self._feature_patterns_cache: Optional[List[FeaturePattern]] = None
        self._cache_loaded = False

    async def create_feature_extraction_run(self, run: FeatureExtractionRun) -> int:
        """Create a new feature extraction run record."""
        async with self.environment.get_connection() as conn:
            query = f"""
            INSERT INTO {self.table_prefix}_feature_extraction_runs (
                run_id, run_type, status, feature_groups, date_range_start, date_range_end,
                total_instruments, total_features_generated, execution_duration_seconds,
                command_line, git_commit_hash, environment, parameters, results, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id
            """
            
            return await conn.fetchval(
                query,
                run.run_id,
                run.run_type,
                run.status,
                run.feature_groups,
                run.date_range_start,
                run.date_range_end,
                run.total_instruments,
                run.total_features_generated,
                run.execution_duration_seconds,
                run.command_line,
                run.git_commit_hash,
                run.environment,
                run.parameters,
                run.results,
                run.error_message
            )

    async def update_feature_extraction_run_status(self, run_id: int, status: str, 
                                                  results: Optional[Dict[str, Any]] = None,
                                                  error_message: Optional[str] = None) -> None:
        """Update feature extraction run status."""
        async with self.environment.get_connection() as conn:
            query = f"""
            UPDATE {self.table_prefix}_feature_extraction_runs 
            SET status = $2, results = $3, error_message = $4, updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            """
            
            await conn.execute(query, run_id, status, results, error_message)

    async def create_feature_extraction_instrument(self, instrument: FeatureExtractionInstrument) -> int:
        """Create a new feature extraction instrument record."""
        async with self.environment.get_connection() as conn:
            query = f"""
            INSERT INTO {self.table_prefix}_feature_extraction_instruments (
                run_id, instrument_id, symbol, status, features_generated,
                processing_duration_seconds, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """
            
            return await conn.fetchval(
                query,
                instrument.run_id,
                instrument.instrument_id,
                instrument.symbol,
                instrument.status,
                instrument.features_generated,
                instrument.processing_duration_seconds,
                instrument.error_message
            )

    async def create_feature_availability(self, availability: FeatureAvailability) -> int:
        """Create or update feature availability record."""
        async with self.environment.get_connection() as conn:
            query = f"""
            INSERT INTO {self.table_prefix}_feature_availability (
                feature_group_id, instrument_id, symbol, year_month, file_path,
                file_size_bytes, record_count, date_range_start, date_range_end,
                quality_score, validation_status, validation_errors
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (feature_group_id, instrument_id, year_month) DO UPDATE SET
                file_path = EXCLUDED.file_path,
                file_size_bytes = EXCLUDED.file_size_bytes,
                record_count = EXCLUDED.record_count,
                date_range_start = EXCLUDED.date_range_start,
                date_range_end = EXCLUDED.date_range_end,
                quality_score = EXCLUDED.quality_score,
                validation_status = EXCLUDED.validation_status,
                validation_errors = EXCLUDED.validation_errors,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """
            
            return await conn.fetchval(
                query,
                availability.feature_group_id,
                availability.instrument_id,
                availability.symbol,
                availability.year_month,
                availability.file_path,
                availability.file_size_bytes,
                availability.record_count,
                availability.date_range_start,
                availability.date_range_end,
                availability.quality_score,
                availability.validation_status,
                availability.validation_errors
            )

    async def get_feature_groups(self, active_only: bool = True) -> List[FeatureGroup]:
        """Get all feature groups."""
        async with self.environment.get_connection() as conn:
            where_clause = "WHERE is_active = true" if active_only else ""
            query = f"""
            SELECT id, group_name, display_name, description, category, update_frequency,
                   computation_lag_minutes, dependencies, storage_format, retention_months,
                   is_active, created_at, updated_at
            FROM {self.table_prefix}_feature_groups
            {where_clause}
            ORDER BY group_name
            """
            
            rows = await conn.fetch(query)
            return [
                FeatureGroup(
                    id=row['id'],
                    group_name=row['group_name'],
                    display_name=row['display_name'],
                    description=row['description'],
                    category=row['category'],
                    update_frequency=row['update_frequency'],
                    computation_lag_minutes=row['computation_lag_minutes'],
                    dependencies=row['dependencies'] or [],
                    storage_format=row['storage_format'],
                    retention_months=row['retention_months'],
                    is_active=row['is_active'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    async def get_feature_catalog_by_group(self, group_name: str) -> List[FeatureCatalog]:
        """Get feature catalog for a specific group."""
        async with self.environment.get_connection() as conn:
            query = f"""
            SELECT fc.feature_id, fc.feature_name, fc.feature_group_id, fc.data_type,
                   fc.column_position, fc.description, fc.computation_method,
                   fc.dependencies, fc.validation_rules, fc.is_active,
                   fc.created_at, fc.updated_at
            FROM {self.table_prefix}_feature_catalog fc
            JOIN {self.table_prefix}_feature_groups fg ON fc.feature_group_id = fg.id
            WHERE fg.group_name = $1 AND fc.is_active = true
            ORDER BY fc.column_position
            """
            
            rows = await conn.fetch(query, group_name)
            return [
                FeatureCatalog(
                    feature_id=row['feature_id'],
                    feature_name=row['feature_name'],
                    feature_group_id=row['feature_group_id'],
                    data_type=row['data_type'],
                    column_position=row['column_position'],
                    description=row['description'],
                    computation_method=row['computation_method'],
                    dependencies=row['dependencies'] or [],
                    validation_rules=row['validation_rules'],
                    is_active=row['is_active'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    async def get_feature_availability_coverage(self, symbols: List[str], 
                                              feature_groups: List[str],
                                              start_date: date, end_date: date) -> List[FeatureAvailability]:
        """Get feature availability coverage for discovery dashboard."""
        async with self.environment.get_connection() as conn:
            query = f"""
            SELECT fa.id, fa.feature_group_id, fa.instrument_id, fa.symbol, fa.year_month,
                   fa.file_path, fa.file_size_bytes, fa.record_count, fa.date_range_start,
                   fa.date_range_end, fa.quality_score, fa.validation_status,
                   fa.validation_errors, fa.created_at, fa.updated_at,
                   fg.group_name, fg.display_name
            FROM {self.table_prefix}_feature_availability fa
            JOIN {self.table_prefix}_feature_groups fg ON fa.feature_group_id = fg.id
            WHERE fa.symbol = ANY($1)
              AND fg.group_name = ANY($2) 
              AND fa.year_month >= $3
              AND fa.year_month <= $4
              AND fa.validation_status = 'passed'
            ORDER BY fa.symbol, fg.group_name, fa.year_month
            """
            
            rows = await conn.fetch(query, symbols, feature_groups, start_date, end_date)
            return [
                FeatureAvailability(
                    id=row['id'],
                    feature_group_id=row['feature_group_id'],
                    instrument_id=row['instrument_id'],
                    symbol=row['symbol'],
                    year_month=row['year_month'],
                    file_path=row['file_path'],
                    file_size_bytes=row['file_size_bytes'],
                    record_count=row['record_count'],
                    date_range_start=row['date_range_start'],
                    date_range_end=row['date_range_end'],
                    quality_score=row['quality_score'],
                    validation_status=row['validation_status'],
                    validation_errors=row['validation_errors'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    async def get_recent_extraction_runs(self, limit: int = 10) -> List[FeatureExtractionRun]:
        """Get recent feature extraction runs."""
        async with self.environment.get_connection() as conn:
            query = f"""
            SELECT id, run_id, run_type, status, feature_groups, date_range_start, date_range_end,
                   total_instruments, total_features_generated, execution_duration_seconds,
                   created_at, updated_at, command_line, git_commit_hash, environment,
                   parameters, results, error_message
            FROM {self.table_prefix}_feature_extraction_runs
            ORDER BY created_at DESC
            LIMIT $1
            """
            
            rows = await conn.fetch(query, limit)
            return [
                FeatureExtractionRun(
                    id=row['id'],
                    run_id=row['run_id'],
                    run_type=row['run_type'],
                    status=row['status'],
                    feature_groups=row['feature_groups'] or [],
                    date_range_start=row['date_range_start'],
                    date_range_end=row['date_range_end'],
                    total_instruments=row['total_instruments'],
                    total_features_generated=row['total_features_generated'],
                    execution_duration_seconds=row['execution_duration_seconds'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    command_line=row['command_line'],
                    git_commit_hash=row['git_commit_hash'],
                    environment=row['environment'],
                    parameters=row['parameters'],
                    results=row['results'],
                    error_message=row['error_message']
                )
                for row in rows
            ]

    async def get_feature_group_mapping(self, feature_name: str) -> FeatureMappingResult:
        """Map feature name to feature group using database patterns."""
        await self._ensure_cache_loaded()
        
        # Try exact match first
        if feature_name in self._feature_catalog_cache:
            catalog_entry = self._feature_catalog_cache[feature_name]
            group = self._feature_groups_cache.get(catalog_entry.feature_group_id)
            if group:
                return FeatureMappingResult(
                    feature_name=feature_name,
                    feature_group_name=group.group_name,
                    feature_group_id=group.id,
                    match_type="exact",
                    confidence=1.0
                )
        
        # Try pattern matching
        for pattern in self._feature_patterns_cache:
            match_result = self._test_pattern_match(feature_name, pattern)
            if match_result:
                group = self._feature_groups_cache.get(pattern.feature_group_id)
                if group:
                    return FeatureMappingResult(
                        feature_name=feature_name,
                        feature_group_name=group.group_name,
                        feature_group_id=pattern.feature_group_id,
                        match_type="pattern",
                        pattern_matched=pattern.pattern,
                        confidence=max(0.6, 1.0 - (pattern.priority / 1000.0))
                    )
        
        # Default fallback to ohlcv_basic
        default_group = next(
            (g for g in self._feature_groups_cache.values() if g.group_name == "ohlcv_basic"),
            None
        )
        if default_group:
            return FeatureMappingResult(
                feature_name=feature_name,
                feature_group_name=default_group.group_name,
                feature_group_id=default_group.id,
                match_type="default",
                confidence=0.3
            )
        
        # Ultimate fallback
        return FeatureMappingResult(
            feature_name=feature_name,
            feature_group_name="unknown",
            feature_group_id=0,
            match_type="unknown",
            confidence=0.0
        )

    async def get_feature_mappings_batch(self, feature_names: List[str]) -> List[FeatureMappingResult]:
        """Get feature group mappings for multiple features efficiently."""
        await self._ensure_cache_loaded()
        
        results = []
        for feature_name in feature_names:
            mapping = await self.get_feature_group_mapping(feature_name)
            results.append(mapping)
        
        return results

    async def _ensure_cache_loaded(self) -> None:
        """Ensure all caches are loaded."""
        if not self._cache_loaded:
            await self._load_feature_groups_cache()
            await self._load_feature_catalog_cache()
            await self._load_feature_patterns_cache()
            self._cache_loaded = True

    async def _load_feature_groups_cache(self) -> None:
        """Load feature groups into cache."""
        groups = await self.get_feature_groups(active_only=True)
        self._feature_groups_cache = {group.id: group for group in groups}
        self.logger.debug(f"Loaded {len(groups)} feature groups into cache")

    async def _load_feature_catalog_cache(self) -> None:
        """Load feature catalog into cache."""
        async with self.environment.get_connection() as conn:
            query = f"""
            SELECT feature_name, feature_group_id, data_type, column_position,
                   description, computation_method, dependencies, validation_rules
            FROM {self.table_prefix}_feature_catalog
            WHERE is_active = true
            """
            
            rows = await conn.fetch(query)
            self._feature_catalog_cache = {}
            
            for row in rows:
                catalog_entry = FeatureCatalog(
                    feature_name=row['feature_name'],
                    feature_group_id=row['feature_group_id'],
                    data_type=row['data_type'],
                    column_position=row['column_position'],
                    description=row['description'],
                    computation_method=row['computation_method'],
                    dependencies=row['dependencies'] or [],
                    validation_rules=row['validation_rules']
                )
                self._feature_catalog_cache[row['feature_name']] = catalog_entry
        
        self.logger.debug(f"Loaded {len(self._feature_catalog_cache)} features into catalog cache")

    async def _load_feature_patterns_cache(self) -> None:
        """Load feature patterns into cache, sorted by priority."""
        async with self.environment.get_connection() as conn:
            query = f"""
            SELECT id, pattern, feature_group_id, pattern_type, priority, description
            FROM {self.table_prefix}_feature_patterns
            ORDER BY priority ASC
            """
            
            rows = await conn.fetch(query)
            self._feature_patterns_cache = [
                FeaturePattern(
                    id=row['id'],
                    pattern=row['pattern'],
                    feature_group_id=row['feature_group_id'],
                    pattern_type=row['pattern_type'],
                    priority=row['priority'],
                    description=row['description']
                )
                for row in rows
            ]
        
        self.logger.debug(f"Loaded {len(self._feature_patterns_cache)} feature patterns into cache")

    def _test_pattern_match(self, feature_name: str, pattern: FeaturePattern) -> bool:
        """Test if feature name matches pattern."""
        try:
            if pattern.pattern_type == "exact":
                return feature_name == pattern.pattern
            elif pattern.pattern_type == "starts_with":
                return feature_name.startswith(pattern.pattern)
            elif pattern.pattern_type == "ends_with":
                return feature_name.endswith(pattern.pattern)
            elif pattern.pattern_type == "contains":
                return pattern.pattern in feature_name
            elif pattern.pattern_type == "regex":
                return bool(re.match(pattern.pattern, feature_name))
            else:
                self.logger.warning(f"Unknown pattern type: {pattern.pattern_type}")
                return False
        except Exception as e:
            self.logger.warning(f"Pattern matching error for {feature_name} with {pattern.pattern}: {e}")
            return False