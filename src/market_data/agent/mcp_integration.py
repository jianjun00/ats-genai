import os
import logging
import asyncio
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta

from .data_agent_orchestrator import DataAgentOrchestrator
from .polygon_adapter import PolygonAdapter
from .tiingo_adapter import TiingoAdapter
from .reconciliation import ReconciliationEngine
from .llm_assistant import LLMAssistant

logger = logging.getLogger(__name__)

class MCPToolRegistry:
    """
    Registry for MCP tools related to the data agent.
    Provides a centralized interface for registering and executing tools.
    """
    
    def __init__(self):
        self.tools = {}
        self.orchestrator = None
        self.llm_assistant = None
    
    def register_tool(self, name: str, tool_function):
        """
        Register a tool with the registry.
        
        Args:
            name: Name of the tool
            tool_function: Function to execute when the tool is called
        """
        self.tools[name] = tool_function
        logger.info(f"Registered tool: {name}")
    
    async def execute_tool(self, tool_name: str, params: Dict[str, Any]):
        """
        Execute a registered tool.
        
        Args:
            tool_name: Name of the tool to execute
            params: Parameters to pass to the tool
            
        Returns:
            Result of the tool execution
        """
        if tool_name not in self.tools:
            raise ValueError(f"Tool {tool_name} not found")
        
        logger.info(f"Executing tool: {tool_name} with params: {params}")
        return await self.tools[tool_name](**params)
    
    async def initialize_data_agent(self, pool, config: Dict[str, Any] = None):
        """
        Initialize the data agent with the provided configuration.
        
        Args:
            pool: Database connection pool
            config: Configuration for the data agent
        """
        config = config or {}
        
        # Initialize adapters
        adapters = {}
        
        # Polygon adapter
        polygon_api_key = config.get("polygon_api_key") or os.getenv("POLYGON_API_KEY")
        if polygon_api_key:
            adapters["polygon"] = PolygonAdapter(api_key=polygon_api_key)
        
        # Tiingo adapter
        tiingo_api_key = config.get("tiingo_api_key") or os.getenv("TIINGO_API_KEY")
        if tiingo_api_key:
            adapters["tiingo"] = TiingoAdapter(api_key=tiingo_api_key)
        
        # Initialize LLM assistant
        openai_api_key = config.get("openai_api_key") or os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            self.llm_assistant = LLMAssistant(api_key=openai_api_key)
        
        # Initialize reconciliation engine
        vendor_priority = config.get("vendor_priority", ["tiingo", "polygon"])
        reconciliation_engine = ReconciliationEngine(vendor_priority=vendor_priority)
        
        # Initialize orchestrator
        lookback_years = config.get("lookback_years", 5)
        self.orchestrator = DataAgentOrchestrator(
            pool=pool,
            adapters=adapters,
            reconciliation_engine=reconciliation_engine,
            lookback_years=lookback_years
        )
        
        logger.info(f"Initialized data agent with adapters: {list(adapters.keys())}")
        
        # Register tools
        self._register_data_agent_tools()
    
    def _register_data_agent_tools(self):
        """Register all data agent related tools"""
        if not self.orchestrator:
            raise ValueError("Data agent orchestrator not initialized")
        
        # Register backfill tool
        self.register_tool(
            "run_backfill",
            self._run_backfill
        )
        
        # Register frontfill tool
        self.register_tool(
            "run_frontfill",
            self._run_frontfill
        )
        
        # Register get missing data points tool
        self.register_tool(
            "get_missing_data_points",
            self._get_missing_data_points
        )
        
        # Register process data point tool
        self.register_tool(
            "process_data_point",
            self._process_data_point
        )
        
        # Register LLM assistant tools if available
        if self.llm_assistant:
            self.register_tool(
                "select_best_source",
                self._select_best_source
            )
            
            self.register_tool(
                "reconcile_data_conflicts",
                self._reconcile_data_conflicts
            )
            
            self.register_tool(
                "detect_anomalies",
                self._detect_anomalies
            )
    
    async def _run_backfill(self, batch_size: int = 100, max_iterations: Optional[int] = None):
        """Run the backfill loop"""
        if not self.orchestrator:
            raise ValueError("Data agent orchestrator not initialized")
        
        await self.orchestrator.run_backfill_loop(
            batch_size=batch_size,
            max_iterations=max_iterations
        )
        
        return {"status": "completed"}
    
    async def _run_frontfill(self):
        """Run the frontfill loop"""
        if not self.orchestrator:
            raise ValueError("Data agent orchestrator not initialized")
        
        await self.orchestrator.run_frontfill_loop()
        
        return {"status": "completed"}
    
    async def _get_missing_data_points(self, symbols: Optional[List[str]] = None, limit: int = 100):
        """Get missing data points"""
        if not self.orchestrator:
            raise ValueError("Data agent orchestrator not initialized")
        
        missing_points = await self.orchestrator.get_missing_data_points(symbols)
        
        # Limit the number of points returned
        missing_points = missing_points[:limit]
        
        # Convert dates to strings for JSON serialization
        for point in missing_points:
            if isinstance(point["date"], date):
                point["date"] = point["date"].isoformat()
        
        return {"missing_points": missing_points, "total_count": len(missing_points)}
    
    async def _process_data_point(self, symbol: str, date_str: str):
        """Process a specific data point"""
        if not self.orchestrator:
            raise ValueError("Data agent orchestrator not initialized")
        
        # Parse date string
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        data_point = {"symbol": symbol, "date": target_date}
        await self.orchestrator._process_data_point(data_point)
        
        return {"status": "completed", "symbol": symbol, "date": date_str}
    
    async def _select_best_source(self, symbol: str, date_str: str):
        """Select the best data source for a specific data point"""
        if not self.llm_assistant:
            raise ValueError("LLM assistant not initialized")
        
        # Parse date string
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        data_point = {"symbol": symbol, "date": target_date}
        available_sources = list(self.orchestrator.adapters.keys()) if self.orchestrator else []
        
        best_source = self.llm_assistant.select_best_source(data_point, available_sources)
        
        return {"best_source": best_source, "available_sources": available_sources}
    
    async def _reconcile_data_conflicts(self, records_json: str):
        """Reconcile data conflicts"""
        if not self.llm_assistant:
            raise ValueError("LLM assistant not initialized")
        
        # Parse records JSON
        records_data = json.loads(records_json)
        records = []
        
        for record_data in records_data:
            record_date = datetime.strptime(record_data["date"], "%Y-%m-%d").date()
            records.append(
                EODPrice(
                    instrument_id=record_data["symbol"],
                    date=record_date,
                    open=record_data.get("open"),
                    high=record_data.get("high"),
                    low=record_data.get("low"),
                    close=record_data.get("close"),
                    adj_close=record_data.get("adj_close"),
                    volume=record_data.get("volume"),
                    vendor=record_data.get("vendor")
                )
            )
        
        reconciled = self.llm_assistant.reconcile_data_conflicts(records)
        
        return {"reconciled": reconciled}
    
    async def _detect_anomalies(self, record_json: str, history_json: str):
        """Detect anomalies in a record"""
        if not self.llm_assistant:
            raise ValueError("LLM assistant not initialized")
        
        # Parse record and history JSON
        record = json.loads(record_json)
        history = json.loads(history_json)
        
        anomalies = self.llm_assistant.detect_anomalies(record, history)
        
        return {"anomalies": anomalies}
