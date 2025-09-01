"""
Data Quality Monitoring Dashboard for ATS System.
Provides comprehensive monitoring of data quality across all data sources and pipelines.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
import logging
import asyncpg
from enum import Enum
import plotly.express as px
import streamlit as st

logger = logging.getLogger(__name__)


class DataQualityLevel(Enum):
    """Data quality severity levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    WARNING = "warning"
    CRITICAL = "critical"
    FAILURE = "failure"


@dataclass
class DataQualityMetric:
    """Single data quality metric."""
    metric_name: str
    table_name: str
    column_name: Optional[str]
    metric_value: float
    threshold_warning: float
    threshold_critical: float
    quality_level: DataQualityLevel
    message: str
    timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class DataQualityReport:
    """Comprehensive data quality report."""
    report_timestamp: datetime
    overall_score: float
    overall_level: DataQualityLevel
    metrics: List[DataQualityMetric]
    summary_stats: Dict[str, Any]
    recommendations: List[str]


class DataQualityMonitor:
    """Monitor data quality across the ATS system."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env):
        self.pool = connection_pool
        self.env = env
        
        # Quality thresholds
        self.quality_thresholds = {
            'completeness': {'warning': 0.95, 'critical': 0.90},
            'freshness_hours': {'warning': 24, 'critical': 48},
            'accuracy': {'warning': 0.98, 'critical': 0.95},
            'consistency': {'warning': 0.98, 'critical': 0.95},
            'duplicate_rate': {'warning': 0.01, 'critical': 0.05},
            'outlier_rate': {'warning': 0.05, 'critical': 0.10},
            'null_rate': {'warning': 0.02, 'critical': 0.05},
            'schema_compliance': {'warning': 0.99, 'critical': 0.95}
        }
        
        # Tables to monitor
        self.monitored_tables = {
            'instruments': ['symbol', 'name', 'sector', 'market_cap'],
            'daily_prices': ['instrument_id', 'date', 'open', 'high', 'low', 'close', 'volume'],
            'economic_events': ['event_date', 'event_name', 'event_type', 'importance'],
            'universe_states': ['instrument_id', 'date', 'is_active'],
            'residual_returns': ['instrument_id', 'date', 'residual_return', 'r_squared']
        }
    
    async def generate_quality_report(self, lookback_days: int = 7) -> DataQualityReport:
        """Generate comprehensive data quality report."""
        logger.info(f"Generating data quality report for last {lookback_days} days")
        
        start_time = datetime.now()
        all_metrics = []
        
        # Check each monitored table
        for table_name, columns in self.monitored_tables.items():
            table_metrics = await self._check_table_quality(table_name, columns, lookback_days)
            all_metrics.extend(table_metrics)
        
        # Calculate overall score and level
        overall_score = self._calculate_overall_score(all_metrics)
        overall_level = self._determine_quality_level(overall_score, 'overall')
        
        # Generate summary statistics
        summary_stats = self._generate_summary_stats(all_metrics)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(all_metrics)
        
        report = DataQualityReport(
            report_timestamp=start_time,
            overall_score=overall_score,
            overall_level=overall_level,
            metrics=all_metrics,
            summary_stats=summary_stats,
            recommendations=recommendations
        )
        
        logger.info(f"Quality report generated. Overall score: {overall_score:.2f}, Level: {overall_level.value}")
        
        return report
    
    async def _check_table_quality(self, 
                                 table_name: str, 
                                 columns: List[str], 
                                 lookback_days: int) -> List[DataQualityMetric]:
        """Check data quality for a specific table."""
        metrics = []
        qualified_table_name = self.env.get_table_name(table_name)
        
        try:
            async with self.pool.acquire() as conn:
                # Check if table exists
                table_exists = await self._check_table_exists(conn, table_name)
                if not table_exists:
                    metrics.append(DataQualityMetric(
                        metric_name="table_existence",
                        table_name=table_name,
                        column_name=None,
                        metric_value=0.0,
                        threshold_warning=1.0,
                        threshold_critical=1.0,
                        quality_level=DataQualityLevel.FAILURE,
                        message=f"Table {table_name} does not exist",
                        timestamp=datetime.now(),
                        metadata={}
                    ))
                    return metrics
                
                # Check data freshness
                freshness_metric = await self._check_data_freshness(conn, qualified_table_name)
                if freshness_metric:
                    metrics.append(freshness_metric)
                
                # Check completeness
                completeness_metrics = await self._check_data_completeness(conn, qualified_table_name, columns)
                metrics.extend(completeness_metrics)
                
                # Check duplicates
                duplicate_metric = await self._check_duplicates(conn, qualified_table_name, columns)
                if duplicate_metric:
                    metrics.append(duplicate_metric)
                
                # Check outliers (for numeric columns)
                outlier_metrics = await self._check_outliers(conn, qualified_table_name, columns)
                metrics.extend(outlier_metrics)
                
                # Check schema compliance
                schema_metric = await self._check_schema_compliance(conn, qualified_table_name, columns)
                if schema_metric:
                    metrics.append(schema_metric)
                
                # Check row count trends
                count_metric = await self._check_row_count_trends(conn, qualified_table_name, lookback_days)
                if count_metric:
                    metrics.append(count_metric)
        
        except Exception as e:
            logger.error(f"Failed to check quality for table {table_name}: {e}")
            metrics.append(DataQualityMetric(
                metric_name="check_failure",
                table_name=table_name,
                column_name=None,
                metric_value=0.0,
                threshold_warning=1.0,
                threshold_critical=1.0,
                quality_level=DataQualityLevel.FAILURE,
                message=f"Quality check failed: {str(e)}",
                timestamp=datetime.now(),
                metadata={"error": str(e)}
            ))
        
        return metrics
    
    async def _check_table_exists(self, conn: asyncpg.Connection, table_name: str) -> bool:
        """Check if table exists."""
        try:
            result = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = $1
                )
            """, table_name)
            return result
        except:
            return False
    
    async def _check_data_freshness(self, conn: asyncpg.Connection, table_name: str) -> Optional[DataQualityMetric]:
        """Check data freshness (most recent record)."""
        try:
            # Try to find a date column
            date_columns = ['date', 'created_at', 'updated_at', 'event_date', 'timestamp']
            
            for date_col in date_columns:
                try:
                    result = await conn.fetchrow(f"""
                        SELECT MAX({date_col}) as latest_date
                        FROM {table_name}
                        WHERE {date_col} IS NOT NULL
                    """)
                    
                    if result and result['latest_date']:
                        latest_date = result['latest_date']
                        if isinstance(latest_date, datetime):
                            hours_old = (datetime.now() - latest_date).total_seconds() / 3600
                        else:
                            # Handle date-only fields
                            latest_datetime = datetime.combine(latest_date, datetime.min.time())
                            hours_old = (datetime.now() - latest_datetime).total_seconds() / 3600
                        
                        quality_level = self._determine_quality_level(hours_old, 'freshness_hours')
                        
                        return DataQualityMetric(
                            metric_name="data_freshness",
                            table_name=table_name,
                            column_name=date_col,
                            metric_value=hours_old,
                            threshold_warning=self.quality_thresholds['freshness_hours']['warning'],
                            threshold_critical=self.quality_thresholds['freshness_hours']['critical'],
                            quality_level=quality_level,
                            message=f"Latest data is {hours_old:.1f} hours old",
                            timestamp=datetime.now(),
                            metadata={"latest_date": str(latest_date)}
                        )
                except:
                    continue
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to check freshness for {table_name}: {e}")
            return None
    
    async def _check_data_completeness(self, 
                                     conn: asyncpg.Connection, 
                                     table_name: str, 
                                     columns: List[str]) -> List[DataQualityMetric]:
        """Check data completeness (null rates)."""
        metrics = []
        
        try:
            total_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            
            if total_rows == 0:
                return [DataQualityMetric(
                    metric_name="row_count",
                    table_name=table_name,
                    column_name=None,
                    metric_value=0.0,
                    threshold_warning=1.0,
                    threshold_critical=1.0,
                    quality_level=DataQualityLevel.CRITICAL,
                    message="Table is empty",
                    timestamp=datetime.now(),
                    metadata={}
                )]
            
            for column in columns:
                try:
                    null_count = await conn.fetchval(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE {column} IS NULL
                    """)
                    
                    null_rate = null_count / total_rows
                    completeness = 1 - null_rate
                    
                    quality_level = self._determine_quality_level(completeness, 'completeness')
                    
                    metrics.append(DataQualityMetric(
                        metric_name="completeness",
                        table_name=table_name,
                        column_name=column,
                        metric_value=completeness,
                        threshold_warning=self.quality_thresholds['completeness']['warning'],
                        threshold_critical=self.quality_thresholds['completeness']['critical'],
                        quality_level=quality_level,
                        message=f"Column {column} is {completeness:.2%} complete",
                        timestamp=datetime.now(),
                        metadata={"null_count": null_count, "total_rows": total_rows}
                    ))
                    
                except Exception as e:
                    logger.warning(f"Failed to check completeness for {table_name}.{column}: {e}")
        
        except Exception as e:
            logger.warning(f"Failed to check completeness for {table_name}: {e}")
        
        return metrics
    
    async def _check_duplicates(self, 
                              conn: asyncpg.Connection, 
                              table_name: str, 
                              columns: List[str]) -> Optional[DataQualityMetric]:
        """Check for duplicate records."""
        try:
            total_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            
            if total_rows == 0:
                return None
            
            # Use primary key columns or first few columns for duplicate detection
            key_columns = columns[:3]  # Use first 3 columns as composite key
            
            unique_rows = await conn.fetchval(f"""
                SELECT COUNT(DISTINCT ({', '.join(key_columns)}))
                FROM {table_name}
                WHERE {' AND '.join([f'{col} IS NOT NULL' for col in key_columns])}
            """)
            
            duplicate_rate = 1 - (unique_rows / total_rows) if total_rows > 0 else 0
            quality_level = self._determine_quality_level(duplicate_rate, 'duplicate_rate')
            
            return DataQualityMetric(
                metric_name="duplicate_rate",
                table_name=table_name,
                column_name=None,
                metric_value=duplicate_rate,
                threshold_warning=self.quality_thresholds['duplicate_rate']['warning'],
                threshold_critical=self.quality_thresholds['duplicate_rate']['critical'],
                quality_level=quality_level,
                message=f"Duplicate rate: {duplicate_rate:.2%}",
                timestamp=datetime.now(),
                metadata={"total_rows": total_rows, "unique_rows": unique_rows}
            )
            
        except Exception as e:
            logger.warning(f"Failed to check duplicates for {table_name}: {e}")
            return None
    
    async def _check_outliers(self, 
                            conn: asyncpg.Connection, 
                            table_name: str, 
                            columns: List[str]) -> List[DataQualityMetric]:
        """Check for outliers in numeric columns."""
        metrics = []
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'market_cap', 'residual_return']
        
        for column in columns:
            if column in numeric_columns:
                try:
                    stats = await conn.fetchrow(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            AVG({column}) as mean_val,
                            STDDEV({column}) as std_val,
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q25,
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q75
                        FROM {table_name}
                        WHERE {column} IS NOT NULL
                    """)
                    
                    if stats and stats['total_count'] > 0:
                        # Use IQR method for outlier detection
                        q25, q75 = stats['q25'], stats['q75']
                        iqr = q75 - q25
                        lower_bound = q25 - 1.5 * iqr
                        upper_bound = q75 + 1.5 * iqr
                        
                        outlier_count = await conn.fetchval(f"""
                            SELECT COUNT(*)
                            FROM {table_name}
                            WHERE {column} IS NOT NULL
                            AND ({column} < $1 OR {column} > $2)
                        """, lower_bound, upper_bound)
                        
                        outlier_rate = outlier_count / stats['total_count']
                        quality_level = self._determine_quality_level(outlier_rate, 'outlier_rate')
                        
                        metrics.append(DataQualityMetric(
                            metric_name="outlier_rate",
                            table_name=table_name,
                            column_name=column,
                            metric_value=outlier_rate,
                            threshold_warning=self.quality_thresholds['outlier_rate']['warning'],
                            threshold_critical=self.quality_thresholds['outlier_rate']['critical'],
                            quality_level=quality_level,
                            message=f"Outlier rate in {column}: {outlier_rate:.2%}",
                            timestamp=datetime.now(),
                            metadata={
                                "outlier_count": outlier_count,
                                "total_count": stats['total_count'],
                                "lower_bound": float(lower_bound),
                                "upper_bound": float(upper_bound)
                            }
                        ))
                
                except Exception as e:
                    logger.warning(f"Failed to check outliers for {table_name}.{column}: {e}")
        
        return metrics
    
    async def _check_schema_compliance(self, 
                                     conn: asyncpg.Connection, 
                                     table_name: str, 
                                     expected_columns: List[str]) -> Optional[DataQualityMetric]:
        """Check schema compliance (column existence)."""
        try:
            # Get actual columns
            actual_columns = await conn.fetch(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1
            """, table_name.split('.')[-1])
            
            actual_column_names = {row['column_name'] for row in actual_columns}
            expected_column_set = set(expected_columns)
            
            # Calculate compliance
            missing_columns = expected_column_set - actual_column_names
            compliance_rate = (len(expected_column_set) - len(missing_columns)) / len(expected_column_set)
            
            quality_level = self._determine_quality_level(compliance_rate, 'schema_compliance')
            
            return DataQualityMetric(
                metric_name="schema_compliance",
                table_name=table_name,
                column_name=None,
                metric_value=compliance_rate,
                threshold_warning=self.quality_thresholds['schema_compliance']['warning'],
                threshold_critical=self.quality_thresholds['schema_compliance']['critical'],
                quality_level=quality_level,
                message=f"Schema compliance: {compliance_rate:.2%}",
                timestamp=datetime.now(),
                metadata={
                    "missing_columns": list(missing_columns),
                    "expected_columns": expected_columns,
                    "actual_columns": list(actual_column_names)
                }
            )
            
        except Exception as e:
            logger.warning(f"Failed to check schema compliance for {table_name}: {e}")
            return None
    
    async def _check_row_count_trends(self, 
                                    conn: asyncpg.Connection, 
                                    table_name: str, 
                                    lookback_days: int) -> Optional[DataQualityMetric]:
        """Check row count trends over time."""
        try:
            # Check if table has a date column
            date_columns = ['date', 'created_at', 'event_date']
            date_column = None
            
            for col in date_columns:
                try:
                    await conn.fetchval(f"SELECT {col} FROM {table_name} LIMIT 1")
                    date_column = col
                    break
                except:
                    continue
            
            if not date_column:
                return None
            
            # Get daily row counts
            daily_counts = await conn.fetch(f"""
                SELECT 
                    DATE({date_column}) as date,
                    COUNT(*) as row_count
                FROM {table_name}
                WHERE {date_column} >= NOW() - INTERVAL '{lookback_days} days'
                GROUP BY DATE({date_column})
                ORDER BY date DESC
            """)
            
            if len(daily_counts) < 2:
                return None
            
            counts = [row['row_count'] for row in daily_counts]
            
            # Check for significant drops
            recent_avg = np.mean(counts[:3]) if len(counts) >= 3 else counts[0]
            historical_avg = np.mean(counts[3:]) if len(counts) > 3 else recent_avg
            
            if historical_avg > 0:
                trend_ratio = recent_avg / historical_avg
            else:
                trend_ratio = 1.0
            
            # Determine quality level based on trend
            if trend_ratio < 0.5:
                quality_level = DataQualityLevel.CRITICAL
            elif trend_ratio < 0.8:
                quality_level = DataQualityLevel.WARNING
            else:
                quality_level = DataQualityLevel.GOOD
            
            return DataQualityMetric(
                metric_name="row_count_trend",
                table_name=table_name,
                column_name=date_column,
                metric_value=trend_ratio,
                threshold_warning=0.8,
                threshold_critical=0.5,
                quality_level=quality_level,
                message=f"Recent row count trend: {trend_ratio:.2f}x historical average",
                timestamp=datetime.now(),
                metadata={
                    "recent_avg": recent_avg,
                    "historical_avg": historical_avg,
                    "daily_counts": counts[:7]  # Last 7 days
                }
            )
            
        except Exception as e:
            logger.warning(f"Failed to check row count trends for {table_name}: {e}")
            return None
    
    def _determine_quality_level(self, metric_value: float, metric_type: str) -> DataQualityLevel:
        """Determine quality level based on metric value and type."""
        thresholds = self.quality_thresholds.get(metric_type, {})
        warning_threshold = thresholds.get('warning', 0.9)
        critical_threshold = thresholds.get('critical', 0.8)
        
        # For metrics where higher is better (completeness, accuracy, etc.)
        if metric_type in ['completeness', 'accuracy', 'consistency', 'schema_compliance']:
            if metric_value >= warning_threshold:
                return DataQualityLevel.EXCELLENT if metric_value >= 0.99 else DataQualityLevel.GOOD
            elif metric_value >= critical_threshold:
                return DataQualityLevel.WARNING
            else:
                return DataQualityLevel.CRITICAL
        
        # For metrics where lower is better (duplicate_rate, outlier_rate, null_rate, freshness_hours)
        else:
            if metric_value <= critical_threshold:
                return DataQualityLevel.EXCELLENT if metric_value <= critical_threshold / 2 else DataQualityLevel.GOOD
            elif metric_value <= warning_threshold:
                return DataQualityLevel.WARNING
            else:
                return DataQualityLevel.CRITICAL
    
    def _calculate_overall_score(self, metrics: List[DataQualityMetric]) -> float:
        """Calculate overall data quality score."""
        if not metrics:
            return 0.0
        
        # Weight different metrics
        weights = {
            'completeness': 0.25,
            'freshness': 0.20,
            'accuracy': 0.20,
            'consistency': 0.15,
            'duplicate_rate': 0.10,
            'outlier_rate': 0.05,
            'schema_compliance': 0.05
        }
        
        weighted_scores = []
        
        for metric in metrics:
            if metric.quality_level == DataQualityLevel.FAILURE:
                return 0.0  # Any failure makes overall score 0
            
            # Convert quality level to numeric score
            level_scores = {
                DataQualityLevel.EXCELLENT: 1.0,
                DataQualityLevel.GOOD: 0.8,
                DataQualityLevel.WARNING: 0.6,
                DataQualityLevel.CRITICAL: 0.3,
                DataQualityLevel.FAILURE: 0.0
            }
            
            score = level_scores[metric.quality_level]
            weight = weights.get(metric.metric_name, 0.1)
            weighted_scores.append(score * weight)
        
        return sum(weighted_scores) / len(weighted_scores) if weighted_scores else 0.0
    
    def _generate_summary_stats(self, metrics: List[DataQualityMetric]) -> Dict[str, Any]:
        """Generate summary statistics from metrics."""
        if not metrics:
            return {}
        
        level_counts = {}
        for level in DataQualityLevel:
            level_counts[level.value] = sum(1 for m in metrics if m.quality_level == level)
        
        metric_counts = {}
        for metric in metrics:
            metric_counts[metric.metric_name] = metric_counts.get(metric.metric_name, 0) + 1
        
        table_scores = {}
        for metric in metrics:
            if metric.table_name not in table_scores:
                table_scores[metric.table_name] = []
            
            level_score = {
                DataQualityLevel.EXCELLENT: 1.0,
                DataQualityLevel.GOOD: 0.8,
                DataQualityLevel.WARNING: 0.6,
                DataQualityLevel.CRITICAL: 0.3,
                DataQualityLevel.FAILURE: 0.0
            }[metric.quality_level]
            
            table_scores[metric.table_name].append(level_score)
        
        table_averages = {
            table: sum(scores) / len(scores)
            for table, scores in table_scores.items()
        }
        
        return {
            'total_metrics': len(metrics),
            'level_distribution': level_counts,
            'metric_type_distribution': metric_counts,
            'table_scores': table_averages,
            'worst_table': min(table_averages.items(), key=lambda x: x[1]) if table_averages else None,
            'best_table': max(table_averages.items(), key=lambda x: x[1]) if table_averages else None
        }
    
    def _generate_recommendations(self, metrics: List[DataQualityMetric]) -> List[str]:
        """Generate recommendations based on quality metrics."""
        recommendations = []
        
        # Group metrics by severity
        critical_metrics = [m for m in metrics if m.quality_level == DataQualityLevel.CRITICAL]
        warning_metrics = [m for m in metrics if m.quality_level == DataQualityLevel.WARNING]
        failure_metrics = [m for m in metrics if m.quality_level == DataQualityLevel.FAILURE]
        
        # Critical issues
        if failure_metrics:
            recommendations.append("🚨 URGENT: Address table existence and access issues immediately")
            for metric in failure_metrics:
                recommendations.append(f"   - Fix {metric.table_name}: {metric.message}")
        
        if critical_metrics:
            recommendations.append("⚠️  HIGH PRIORITY: Address critical data quality issues")
            
            # Group by type
            completeness_issues = [m for m in critical_metrics if m.metric_name == 'completeness']
            if completeness_issues:
                recommendations.append(f"   - Investigate missing data in {len(completeness_issues)} columns")
            
            freshness_issues = [m for m in critical_metrics if m.metric_name == 'data_freshness']
            if freshness_issues:
                recommendations.append("   - Check data ingestion pipelines for delays")
            
            duplicate_issues = [m for m in critical_metrics if m.metric_name == 'duplicate_rate']
            if duplicate_issues:
                recommendations.append("   - Implement duplicate detection and removal")
        
        # Warning issues
        if warning_metrics:
            recommendations.append("📋 MEDIUM PRIORITY: Monitor and improve warning-level issues")
            
            outlier_issues = [m for m in warning_metrics if m.metric_name == 'outlier_rate']
            if outlier_issues:
                recommendations.append("   - Review data validation rules for outlier detection")
        
        # General recommendations
        if not recommendations:
            recommendations.append("✅ Data quality is good! Continue monitoring regularly")
        else:
            recommendations.append("📊 Set up automated alerts for critical quality degradation")
            recommendations.append("🔄 Schedule regular data quality reviews")
        
        return recommendations


class DataQualityDashboard:
    """Streamlit dashboard for data quality monitoring."""
    
    def __init__(self, monitor: DataQualityMonitor):
        self.monitor = monitor
    
    def render_dashboard(self, report: DataQualityReport):
        """Render the complete data quality dashboard."""
        st.set_page_config(
            page_title="ATS Data Quality Dashboard",
            page_icon="📊",
            layout="wide"
        )
        
        st.title("🔍 ATS Data Quality Dashboard")
        st.markdown(f"*Report generated at {report.report_timestamp.strftime('%Y-%m-%d %H:%M:%S')}*")
        
        # Overall score and status
        self._render_overall_status(report)
        
        # Summary metrics
        self._render_summary_metrics(report)
        
        # Detailed metrics table
        self._render_detailed_metrics(report)
        
        # Recommendations
        self._render_recommendations(report)
        
        # Trends and visualizations
        self._render_visualizations(report)
    
    def _render_overall_status(self, report: DataQualityReport):
        """Render overall data quality status."""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Overall score gauge
            score_color = {
                DataQualityLevel.EXCELLENT: "green",
                DataQualityLevel.GOOD: "lightgreen", 
                DataQualityLevel.WARNING: "orange",
                DataQualityLevel.CRITICAL: "red",
                DataQualityLevel.FAILURE: "darkred"
            }[report.overall_level]
            
            st.metric(
                "Overall Quality Score",
                f"{report.overall_score:.1%}",
                delta=f"{report.overall_level.value.title()}",
                delta_color="normal"
            )
        
        with col2:
            st.metric(
                "Total Metrics Checked",
                len(report.metrics),
                delta=f"{len([m for m in report.metrics if m.quality_level in [DataQualityLevel.CRITICAL, DataQualityLevel.FAILURE]])} issues"
            )
        
        with col3:
            tables_checked = len(set(m.table_name for m in report.metrics))
            st.metric(
                "Tables Monitored",
                tables_checked,
                delta=f"{len(self.monitor.monitored_tables)} configured"
            )
    
    def _render_summary_metrics(self, report: DataQualityReport):
        """Render summary metrics charts."""
        st.subheader("📊 Quality Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality level distribution
            level_counts = report.summary_stats.get('level_distribution', {})
            
            if level_counts:
                fig_pie = px.pie(
                    values=list(level_counts.values()),
                    names=list(level_counts.keys()),
                    title="Quality Level Distribution",
                    color_discrete_map={
                        'excellent': '#2E8B57',
                        'good': '#90EE90',
                        'warning': '#FFA500',
                        'critical': '#FF6347',
                        'failure': '#8B0000'
                    }
                )
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Table scores
            table_scores = report.summary_stats.get('table_scores', {})
            
            if table_scores:
                fig_bar = px.bar(
                    x=list(table_scores.keys()),
                    y=list(table_scores.values()),
                    title="Quality Score by Table",
                    labels={'x': 'Table', 'y': 'Quality Score'},
                    color=list(table_scores.values()),
                    color_continuous_scale='RdYlGn'
                )
                fig_bar.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_bar, use_container_width=True)
    
    def _render_detailed_metrics(self, report: DataQualityReport):
        """Render detailed metrics table."""
        st.subheader("📋 Detailed Quality Metrics")
        
        # Create metrics DataFrame
        metrics_data = []
        for metric in report.metrics:
            metrics_data.append({
                'Table': metric.table_name.split('.')[-1],
                'Column': metric.column_name or 'All',
                'Metric': metric.metric_name,
                'Value': f"{metric.metric_value:.3f}",
                'Level': metric.quality_level.value.title(),
                'Message': metric.message
            })
        
        if metrics_data:
            df = pd.DataFrame(metrics_data)
            
            # Color code by quality level
            def color_level(val):
                colors = {
                    'Excellent': 'background-color: #2E8B57; color: white',
                    'Good': 'background-color: #90EE90; color: black',
                    'Warning': 'background-color: #FFA500; color: black',
                    'Critical': 'background-color: #FF6347; color: white',
                    'Failure': 'background-color: #8B0000; color: white'
                }
                return colors.get(val, '')
            
            styled_df = df.style.applymap(color_level, subset=['Level'])
            st.dataframe(styled_df, use_container_width=True)
            
            # Filter options
            st.subheader("🔍 Filter Metrics")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                selected_tables = st.multiselect(
                    "Filter by Table",
                    options=df['Table'].unique(),
                    default=df['Table'].unique()
                )
            
            with col2:
                selected_levels = st.multiselect(
                    "Filter by Quality Level",
                    options=df['Level'].unique(),
                    default=df['Level'].unique()
                )
            
            with col3:
                selected_metrics = st.multiselect(
                    "Filter by Metric Type",
                    options=df['Metric'].unique(),
                    default=df['Metric'].unique()
                )
            
            # Apply filters
            filtered_df = df[
                (df['Table'].isin(selected_tables)) &
                (df['Level'].isin(selected_levels)) &
                (df['Metric'].isin(selected_metrics))
            ]
            
            if not filtered_df.empty:
                st.dataframe(filtered_df.style.applymap(color_level, subset=['Level']), use_container_width=True)
        
        else:
            st.warning("No metrics data available")
    
    def _render_recommendations(self, report: DataQualityReport):
        """Render recommendations section."""
        st.subheader("💡 Recommendations")
        
        if report.recommendations:
            for recommendation in report.recommendations:
                if recommendation.startswith("🚨"):
                    st.error(recommendation)
                elif recommendation.startswith("⚠️"):
                    st.warning(recommendation)
                elif recommendation.startswith("📋"):
                    st.info(recommendation)
                elif recommendation.startswith("✅"):
                    st.success(recommendation)
                else:
                    st.write(recommendation)
        else:
            st.info("No specific recommendations at this time.")
    
    def _render_visualizations(self, report: DataQualityReport):
        """Render additional visualizations."""
        st.subheader("📈 Quality Trends & Analysis")
        
        # Metric value distribution
        if report.metrics:
            metric_values = [m.metric_value for m in report.metrics if m.metric_name in ['completeness', 'accuracy']]
            
            if metric_values:
                fig_hist = px.histogram(
                    x=metric_values,
                    nbins=20,
                    title="Distribution of Quality Metric Values",
                    labels={'x': 'Metric Value', 'y': 'Count'}
                )
                st.plotly_chart(fig_hist, use_container_width=True)
        
        # Export options
        st.subheader("📤 Export Options")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Export Report as JSON"):
                report_dict = {
                    'timestamp': report.report_timestamp.isoformat(),
                    'overall_score': report.overall_score,
                    'overall_level': report.overall_level.value,
                    'summary_stats': report.summary_stats,
                    'recommendations': report.recommendations,
                    'metrics': [
                        {
                            'metric_name': m.metric_name,
                            'table_name': m.table_name,
                            'column_name': m.column_name,
                            'metric_value': m.metric_value,
                            'quality_level': m.quality_level.value,
                            'message': m.message
                        }
                        for m in report.metrics
                    ]
                }
                
                st.download_button(
                    "Download JSON Report",
                    data=pd.Series(report_dict).to_json(),
                    file_name=f"data_quality_report_{report.report_timestamp.strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )


# Convenience function for running the dashboard
async def run_data_quality_dashboard(connection_pool: asyncpg.Pool, env):
    """Run the data quality monitoring dashboard."""
    monitor = DataQualityMonitor(connection_pool, env)
    dashboard = DataQualityDashboard(monitor)
    
    # Generate report
    report = await monitor.generate_quality_report(lookback_days=7)
    
    # Render dashboard
    dashboard.render_dashboard(report)
    
    return report