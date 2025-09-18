"""
Instrument Data Agent MCP Integration

This module integrates the Instrument Data Agent with the MCP server.
"""

import logging
from datetime import datetime
from typing import Dict, Any

from core.shared.utils.environment import Environment
from domains.market_data.services.agent.instrument_data_agent import InstrumentDataAgent

logger = logging.getLogger("instrument_mcp_integration")


class InstrumentMCPTools:
    """
    MCP tools for the Instrument Data Agent.
    """
    def __init__(self, env: Environment, debug: bool = False):
        self.env = env
        self.debug = debug
        self.agent = InstrumentDataAgent(env, debug=debug)

    async def run_instrument_backfill(self, **kwargs) -> Dict[str, Any]:
        """
        Run a one-time backfill of instrument data.

        Returns:
            Dict containing the results of the backfill operation.
        """
        logger.info("MCP: Starting instrument backfill")
        result = await self.agent.run_backfill()
        logger.info(f"MCP: Backfill completed: {result['status']}")
        return result

    async def run_instrument_daily_update(self, **kwargs) -> Dict[str, Any]:
        """
        Run daily update of instrument data.

        Returns:
            Dict containing the results of the daily update operation.
        """
        logger.info("MCP: Starting daily instrument update")

        # Check if market is closed
        if not await self.agent.is_market_closed():
            logger.info("MCP: Market is still open. Daily update should run after market close.")
            return {
                "status": "skipped",
                "reason": "Market is still open",
                "timestamp": datetime.now().isoformat()
            }

        result = await self.agent.run_daily_update()
        logger.info(f"MCP: Daily update completed: {result['status']}")
        return result

    async def get_instrument_stats(self, **kwargs) -> Dict[str, Any]:
        """
        Get statistics about the instrument tables.

        Returns:
            Dict containing statistics about the instrument tables.
        """
        logger.info("MCP: Getting instrument statistics")

        try:
            # Get counts from DAOs
            polygon_count = await self.agent.polygon_dao.count_instruments()
            instruments_count = await self.agent.instruments_dao.count_instruments()
            xrefs_count = await self.agent.xrefs_dao.count_xrefs()

            # Get latest update timestamps
            polygon_latest = await self.agent.polygon_dao.get_latest_update_timestamp()

            stats = {
                "polygon_instruments_count": polygon_count,
                "unified_instruments_count": instruments_count,
                "instrument_xrefs_count": xrefs_count,
                "polygon_latest_update": polygon_latest.isoformat() if polygon_latest else None,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"MCP: Got instrument statistics: {stats}")
            return stats

        except Exception as e:
            logger.error(f"MCP: Error getting instrument statistics: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    async def get_instrument_by_symbol(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """
        Get instrument data for a specific symbol.

        Args:
            symbol: The ticker symbol to look up.

        Returns:
            Dict containing instrument data from both polygon and unified tables.
        """
        logger.info(f"MCP: Getting instrument data for symbol: {symbol}")

        try:
            # Get instrument data from polygon
            polygon_instrument = await self.agent.polygon_dao.get_instrument_by_symbol(symbol)

            # Get instrument data from unified tables
            unified_instrument = await self.agent.instruments_dao.get_instrument_by_symbol(symbol)

            # Get vendor ID for ticker
            ticker_vendor = await self.agent.vendors_dao.get_vendor_by_name('ticker')
            ticker_vendor_id = ticker_vendor['id'] if ticker_vendor else None

            # Get xref data
            xref = None
            if ticker_vendor_id and unified_instrument:
                xref = await self.agent.xrefs_dao.find_xref(ticker_vendor_id, symbol)

            result = {
                "symbol": symbol,
                "polygon_data": polygon_instrument,
                "unified_data": unified_instrument,
                "xref_data": xref,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"MCP: Got instrument data for symbol: {symbol}")
            return result

        except Exception as e:
            logger.error(f"MCP: Error getting instrument data for symbol {symbol}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


def register_instrument_tools(registry, env: Environment, debug: bool = False):
    """
    Register instrument tools with the MCP registry.

    Args:
        registry: The MCP tool registry.
        env: The environment.
        debug: Whether to enable debug mode.
    """
    tools = InstrumentMCPTools(env, debug=debug)

    registry.register_tool(
        "run_instrument_backfill",
        tools.run_instrument_backfill,
        "Run a one-time backfill of instrument data",
        {}
    )

    registry.register_tool(
        "run_instrument_daily_update",
        tools.run_instrument_daily_update,
        "Run daily update of instrument data",
        {}
    )

    registry.register_tool(
        "get_instrument_stats",
        tools.get_instrument_stats,
        "Get statistics about the instrument tables",
        {}
    )

    registry.register_tool(
        "get_instrument_by_symbol",
        tools.get_instrument_by_symbol,
        "Get instrument data for a specific symbol",
        {
            "symbol": {
                "type": "string",
                "description": "The ticker symbol to look up"
            }
        }
    )
