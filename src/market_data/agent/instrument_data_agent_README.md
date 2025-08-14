# Instrument Data Agent

The Instrument Data Agent is responsible for maintaining the `instruments` and `instrument_xrefs` tables by performing a one-time backfill of Polygon instrument data and setting up daily updates after market close.

## Features

- One-time backfill of Polygon instrument data
- Daily updates after market close
- Automatic execution of `populate_instrument_polygon` and `populate_unified_instruments`
- Reporting on updates and changes
- MCP integration for running backfill, daily updates, and querying instrument data

## Components

### InstrumentDataAgent

The main agent class that manages the instrument update plan and executes the necessary steps:

- `run_backfill()`: Performs a one-time backfill of Polygon instrument data
- `run_daily_update()`: Runs daily updates after market close
- `execute_plan()`: Executes the update plan steps
- `generate_report()`: Generates a report on updates and changes
- `is_market_closed()`: Checks if the market is closed

### Runner Script

The `run_instrument_agent.py` script provides a command-line interface for running the agent:

```bash
# Run backfill
python -m src.market_data.agent.run_instrument_agent --backfill

# Run daily update
python -m src.market_data.agent.run_instrument_agent --daily-update

# Start scheduler for daily updates
python -m src.market_data.agent.run_instrument_agent --schedule
```

### MCP Integration

The `instrument_mcp_integration.py` module provides MCP tools for:

- Running backfill
- Running daily updates
- Getting instrument statistics
- Fetching instrument data by symbol

## Usage

### One-time Backfill

To perform a one-time backfill of Polygon instrument data:

```bash
python -m src.market_data.agent.run_instrument_agent --backfill
```

This will:
1. Run `populate_instrument_polygon` to fetch instrument data from Polygon
2. Run `populate_unified_instruments` to copy data to the `instruments` and `instrument_xrefs` tables
3. Generate a report on the updates

### Daily Updates

To run a daily update manually:

```bash
python -m src.market_data.agent.run_instrument_agent --daily-update
```

This will:
1. Check if the market is closed
2. Run `populate_instrument_polygon` to fetch recent instrument data from Polygon
3. Run `populate_unified_instruments` to update the `instruments` and `instrument_xrefs` tables
4. Generate a report on the updates

### Scheduling Daily Updates

To schedule daily updates to run automatically after market close:

```bash
python -m src.market_data.agent.run_instrument_agent --schedule
```

This will start a scheduler that runs daily updates at 4:30 PM ET on weekdays.

### MCP Integration

To register the instrument data agent tools with the MCP server:

```python
from src.market_data.agent.instrument_mcp_integration import register_instrument_tools

register_instrument_tools(mcp_server)
```

This will register the following tools:
- `run_instrument_backfill`
- `run_instrument_daily_update`
- `get_instrument_stats`
- `get_instrument_by_symbol`

## Environment Variables

The agent requires the following environment variables:

- `POLYGON_API_KEY`: API key for Polygon
- Database connection variables as per the project's environment configuration

## Reports

Reports are generated in JSON format and include:
- Number of instruments in the `instrument_polygon` table
- Number of instruments in the `instruments` table
- Number of instrument xrefs in the `instrument_xrefs` table
- Latest update timestamp
- Statistics on new and updated instruments

Reports are saved to the `reports` directory with a timestamp in the filename.
