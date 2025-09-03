import logging
import os
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Set, Union
import asyncpg
from .base_adapter import VendorAdapter
from .models import EODPrice
from .reconciliation import ReconciliationEngine
from .reconciled_record_dao import ReconciledRecordDAO
from .monitoring import DataAgentMetrics, DataAgentMonitor
from .alert_handlers import AlertHandler
from .resilience import with_resilience, CircuitBreakerError
from .health_api import setup_health_api
from .logging_config import setup_logging

logger = logging.getLogger(__name__)

class DataAgentOrchestrator:
    """
    Orchestrates the data collection, reconciliation, and storage processes
    for unified stock price data. Manages both backfill and frontfill operations.
    """
    
    def __init__(
        self, 
        pool: asyncpg.Pool,
        adapters: Dict[str, VendorAdapter],
        reconciliation_engine: ReconciliationEngine,
        lookback_years: int = 5,
        enable_monitoring: bool = True,
        alert_handler: Optional[Union[AlertHandler, List[AlertHandler]]] = None,
        enable_prometheus: bool = False,
        prometheus_port: int = 8000,
        max_retries: int = 3,
        enable_circuit_breaker: bool = True,
        enable_health_api: bool = True,
        health_api_port: int = 8081,
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        json_logs: bool = False
    ):
        """
        Initialize the data agent orchestrator.
        
        Args:
            pool: Database connection pool
            adapters: Dictionary mapping vendor names to adapter instances
            reconciliation_engine: Engine for reconciling data from multiple sources
            lookback_years: Number of years to look back for historical data
            enable_monitoring: Whether to enable metrics collection and monitoring
            alert_handler: Optional alert handler or list of handlers
            enable_prometheus: Whether to enable Prometheus metrics export
            prometheus_port: Port to expose Prometheus metrics on
            max_retries: Maximum number of retries for data fetching operations
            enable_circuit_breaker: Whether to enable circuit breaker for data sources
            enable_health_api: Whether to enable health API endpoints
            health_api_port: Port to expose health API on
            log_level: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Override log file path
            json_logs: Whether to use JSON formatting for logs
        """
        # Set up logging first
        setup_logging(
            log_level=log_level,
            log_file=log_file,
            json_format=json_logs
        )
        self.pool = pool
        self.adapters = adapters
        self.reconciliation_engine = reconciliation_engine
        self.dao = ReconciledRecordDAO(pool)
        self.lookback_years = lookback_years
        self.max_retries = max_retries
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_health_api = enable_health_api
        self.health_api_port = health_api_port
        self.health_api = None
        
        # Initialize monitoring
        self.enable_monitoring = enable_monitoring
        if enable_monitoring:
            self.metrics = DataAgentMetrics()
            
            # Check environment variables for Prometheus configuration
            if enable_prometheus is None:
                enable_prometheus = os.environ.get("ENABLE_PROMETHEUS", "").lower() in ("true", "1", "yes")
                
            if prometheus_port is None and os.environ.get("PROMETHEUS_PORT"):
                try:
                    prometheus_port = int(os.environ.get("PROMETHEUS_PORT", "8000"))
                except ValueError:
                    prometheus_port = 8000
            
            # Create and start the monitor
            self.monitor = DataAgentMonitor(
                self.metrics,
                alert_handler=alert_handler,
                enable_prometheus=enable_prometheus,
                prometheus_port=prometheus_port
            )
            
            # Only start monitoring if we're not in a test environment
            # This is determined by checking if there's a running event loop
            try:
                import asyncio
                asyncio.get_running_loop()
                self.monitor.start()
            except RuntimeError:
                # No running event loop, likely in a test environment
                logger.info("No running event loop detected, monitoring will not start automatically")
                # In tests, the monitoring will be started manually if needed
            
            # Store a reference to the monitor in metrics for decorator access
            self.metrics.monitor = self.monitor
            
            # Check environment variables for health API configuration
            if enable_health_api is None:
                enable_health_api = os.environ.get("ENABLE_HEALTH_API", "").lower() in ("true", "1", "yes")
                
            if health_api_port is None and os.environ.get("HEALTH_API_PORT"):
                try:
                    health_api_port = int(os.environ.get("HEALTH_API_PORT", "8081"))
                except ValueError:
                    health_api_port = 8081
            
            # Start health API if enabled
            self.health_api = None
            if enable_health_api:
                # Only start health API if we're not in a test environment
                try:
                    import asyncio
                    asyncio.get_running_loop()
                    # Start health API in a background task
                    asyncio.create_task(self._start_health_api(health_api_port))
                except RuntimeError:
                    # No running event loop, likely in a test environment
                    logger.info("No running event loop detected, health API will not start automatically")
        else:
            self.metrics = None
            self.monitor = None
        
    async def get_all_symbols(self) -> Set[str]:
        """
        Get a set of all symbols to process.
        
        Returns:
            Set of stock symbols
        """
        # In a real implementation, this would fetch from a universe or symbols table
        # For now, we'll use a simple hardcoded list for testing
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT instrument_id FROM reconciled_records
                UNION
                SELECT DISTINCT symbol FROM universe_membership
                WHERE is_member = true
            """)
            return {row['instrument_id'] for row in rows}
    
    async def get_missing_data_points(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Identify missing data points that need to be backfilled.
        
        Args:
            symbols: Optional list of symbols to check. If None, checks all symbols.
            
        Returns:
            List of dictionaries with symbol and date information for missing data
        """
        if symbols is None:
            symbols = await self.get_all_symbols()
            
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365 * self.lookback_years)
        
        missing_points = []
        
        for symbol in symbols:
            # Get existing data for this symbol
            records = await self.dao.list_for_instrument(symbol, "eod")
            existing_dates = {record.as_of.date() if hasattr(record.as_of, 'date') else record.as_of for record in records}
            
            # Generate all dates in range
            current_date = start_date
            while current_date <= end_date:
                if current_date not in existing_dates:
                    missing_points.append({
                        "symbol": symbol,
                        "date": current_date
                    })
                current_date += timedelta(days=1)
                
        return missing_points
    
    async def run_backfill_loop(self, batch_size: int = 100, max_iterations: Optional[int] = None):
        """
        Run the backfill loop to populate missing historical data.
        
        Args:
            batch_size: Number of data points to process in each batch
            max_iterations: Maximum number of iterations to run (None for unlimited)
        """
        # Use metrics directly if available
        start_time = time.time()
        success = False
        
        try:
            iteration = 0
            success = False
            
            while max_iterations is None or iteration < max_iterations:
                # Get missing data points
                missing_points = await self.get_missing_data_points()
                
                # Break if no more missing data points
                if not missing_points:
                    logger.info("No more missing data points, backfill complete")
                    break
                    
                logger.info(f"Found {len(missing_points)} missing data points")
                
                # Process in batches
                for i in range(0, len(missing_points), batch_size):
                    batch = missing_points[i:i+batch_size]
                    await self._process_batch(batch)
                    logger.info(f"Processed batch {i//batch_size + 1}/{(len(missing_points)-1)//batch_size + 1}")
                
                iteration += 1
            
            success = True
            logger.info(f"Backfill loop completed after {iteration} iterations")
        except Exception as e:
            logger.error(f"Error in backfill loop: {e}", exc_info=True)
            success = False
            raise
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            
            # Record final metrics if available
            if self.metrics:
                # Use record_data_point_processed instead of record_batch_processed
                self.metrics.record_data_point_processed(success, elapsed)
        
    async def _start_health_api(self, port: int):
        """
        Start the health API server.
        
        Args:
            port: Port to expose health API on
        """
        try:
            self.health_api = await setup_health_api(
                metrics=self.metrics,
                pool=self.pool,
                adapters=self.adapters,
                port=port
            )
            logger.info(f"Health API started on port {port}")
        except Exception as e:
            logger.error(f"Failed to start health API: {e}")
            self.health_api = None
            
    async def shutdown(self):
        """
        Gracefully shut down the data agent orchestrator.
        
        This stops all background tasks, monitoring, and API servers.
        """
        logger.info("Shutting down data agent orchestrator...")
        
        # Stop health API if running
        if self.health_api:
            try:
                await self.health_api.stop()
                logger.info("Health API stopped")
            except Exception as e:
                logger.error(f"Error stopping health API: {e}")
        
        # Stop monitoring if enabled
        if self.monitor:
            try:
                self.monitor.stop()
                logger.info("Monitoring stopped")
            except Exception as e:
                logger.error(f"Error stopping monitoring: {e}")
        
        # Log final metrics if available
        if self.metrics:
            try:
                self.metrics.log_metrics()
                logger.info("Final metrics logged")
            except Exception as e:
                logger.error(f"Error logging final metrics: {e}")
        
        logger.info("Data agent orchestrator shutdown complete")
        
    @classmethod
    async def create(
        cls,
        db_connection_string: str,
        adapters: Dict[str, VendorAdapter],
        reconciliation_engine: ReconciliationEngine,
        **kwargs
    ):
        """
        Factory method to create a data agent orchestrator with a database pool.
        
        Args:
            db_connection_string: Database connection string
            adapters: Dictionary mapping vendor names to adapter instances
            reconciliation_engine: Engine for reconciling data from multiple sources
            **kwargs: Additional arguments to pass to the constructor
            
        Returns:
            DataAgentOrchestrator instance
        """
        # Create database pool
        pool = await asyncpg.create_pool(db_connection_string)
        
        # Create orchestrator
        return cls(pool, adapters, reconciliation_engine, **kwargs)
    
    async def run_frontfill_loop(self):
        """
        Run the frontfill loop to populate today's data after market close.
        """
        # Use metrics directly if available
        start_time = time.time()
        success = False
        
        try:
            # Check if market is closed
            if not self._is_market_closed():
                logger.info("Market is still open. Skipping frontfill.")
                return
                
            today = datetime.now().date()
            symbols = await self.get_all_symbols()
            
            logger.info(f"Running frontfill for {len(symbols)} symbols")
            
            for symbol in symbols:
                data_point = {"symbol": symbol, "date": today}
                await self._process_data_point(data_point)
                
            logger.info(f"Completed frontfill for {len(symbols)} symbols")
            success = True
        except Exception as e:
            logger.error(f"Error in frontfill loop: {e}", exc_info=True)
            success = False
            raise
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            
            # Record metrics if available
            if self.metrics:
                self.metrics.record_data_point_processed(success, elapsed)
    
    async def _process_batch(self, data_points: List[Dict[str, Any]]):
        """
        Process a batch of data points.
        
        Args:
            data_points: List of data points to process
        """
        # Use metrics directly if available
        start_time = time.time()
        success = False
        
        try:
            logger.info(f"Processing batch {len(data_points)} data points")
            for i, data_point in enumerate(data_points):
                try:
                    await self._process_data_point(data_point)
                except Exception as e:
                    logger.error(f"Error processing data point {i}: {e}", exc_info=True)
            
            logger.info(f"Processed batch {len(data_points)} data points")
            success = True
        except Exception as e:
            logger.error(f"Error processing batch: {e}", exc_info=True)
            success = False
            raise
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            
            # Record metrics if available
            if self.metrics:
                self.metrics.record_data_point_processed(success, elapsed)
    
    async def _process_data_point(self, data_point: Dict[str, Any]):
        """
        Process a single data point by fetching data from all sources,
        reconciling, and storing the result.
        
        Args:
            data_point: Dictionary with symbol and date information
        """
        # Use metrics directly if available
        start_time = time.time()
        success = False
        
        try:
            symbol = data_point["symbol"]
            target_date = data_point["date"]
            
            logger.info(f"Processing data point: {symbol} for {target_date}")
            
            # Fetch from all adapters
            all_prices = []
            
            for vendor_name, adapter in self.adapters.items():
                try:
                    # Apply resilience patterns to data fetching
                    prices = await self._fetch_from_adapter_with_resilience(
                        adapter, vendor_name, symbol, target_date
                    )
                    all_prices.extend(prices)
                        
                except Exception as e:
                    logger.error(f"Error fetching {symbol} data from {vendor_name}: {e}")
                    
                    # Record source failure
                    if self.enable_monitoring and self.metrics:
                        self.metrics.record_source_result(vendor_name, False, 0.0)
            
            if not all_prices:
                logger.warning(f"No data found for {symbol} on {target_date}")
                
                # Record reconciliation metrics
                if self.enable_monitoring and self.metrics:
                    self.metrics.record_reconciliation(0, False)
                
                return
            
            # Reconcile data from multiple sources
            had_conflict = len(all_prices) > 1 and len(set(p.vendor for p in all_prices)) > 1
            reconciled = self.reconciliation_engine.reconcile_eod_prices(all_prices)
            
            # Record reconciliation metrics
            if self.enable_monitoring and self.metrics:
                self.metrics.record_reconciliation(
                    len(set(p.vendor for p in all_prices)),
                    had_conflict
                )
            
            if reconciled:
                # Store the reconciled record
                await self.dao.insert(reconciled)
                logger.info(f"Stored reconciled data for {symbol} on {target_date}")
            
            success = True
        except Exception as e:
            logger.error(f"Error processing data point {data_point.get('symbol', 'unknown')} on {data_point.get('date', 'unknown')}: {e}", exc_info=True)
            success = False
            raise
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            
            # Record metrics if available
            if self.metrics:
                self.metrics.record_data_point_processed(success, elapsed)
        
    def _is_market_closed(self) -> bool:
        """
        Check if the market is closed for the current day.
        
        Returns:
            True if market is closed, False otherwise
        """
        # In a real implementation, this would check market hours and holidays
        # For simplicity, we'll assume market is closed after 4:30 PM ET
        now = datetime.now()
        # Convert to ET (UTC-5 or UTC-4 depending on DST)
        # This is a simplified approach
        et_hour = (now.hour - 5) % 24  # Simplified ET conversion
        return et_hour >= 16 or et_hour < 9  # After 4 PM or before 9 AM
        
    async def _fetch_from_adapter_with_resilience(
        self, adapter: VendorAdapter, vendor_name: str, symbol: str, target_date: date
    ) -> List[EODPrice]:
        """
        Fetch data from an adapter with resilience patterns applied.
        
        Args:
            adapter: The vendor adapter to fetch from
            vendor_name: Name of the vendor/adapter
            symbol: Stock symbol to fetch
            target_date: Date to fetch data for
            
        Returns:
            List of EODPrice objects
        """
        # Define the actual fetch function
        @with_resilience(
            circuit_breaker_name=f"adapter_{vendor_name}" if self.enable_circuit_breaker else None,
            max_retries=self.max_retries,
            retry_exceptions=(Exception,),  # Retry on any exception
            initial_backoff=1.0,
            max_backoff=30.0
        )
        async def _fetch():
            start_time = datetime.now()
            
            # Use a small date range (just the target date)
            prices = adapter.fetch_eod([symbol], target_date, target_date)
            # Filter to exact date match
            prices = [p for p in prices if p.date == target_date]
            
            # Record source metrics
            if self.enable_monitoring:
                elapsed = (datetime.now() - start_time).total_seconds()
                self.metrics.record_source_result(vendor_name, True, elapsed)
                
            return prices
        
        try:
            # Call the resilient fetch function
            return await _fetch()
        except CircuitBreakerError as e:
            # Circuit breaker is open, log and record metrics
            logger.warning(f"Circuit breaker open for {vendor_name}: {e}")
            if self.enable_monitoring:
                self.metrics.record_source_result(vendor_name, False, 0.0)
            return []
        except Exception as e:
            # All retries failed or other error
            logger.error(f"Failed to fetch data from {vendor_name} after retries: {e}")
            raise
