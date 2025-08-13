# Data Agent for Unified US Stock Prices

This module implements a data agent for generating unified daily prices for all US stocks for up to the past five years. The agent integrates data from multiple sources (Tiingo and Polygon) and uses an LLM for tool selection and data reconciliation.

## Architecture

The data agent consists of the following components:

1. **Data Agent Orchestrator**: Manages the overall workflow, including backfill and frontfill loops.
2. **Data Source Adapters**: Interfaces with data providers (Tiingo and Polygon).
3. **Reconciliation Engine**: Reconciles data from multiple sources into a unified record.
4. **LLM Assistant**: Uses an LLM to assist in source selection, conflict resolution, and anomaly detection.
5. **Storage Layer**: Stores reconciled records in a database.
6. **MCP Server Integration**: Hosts tools and the agent on an MCP server.
7. **Monitoring & Metrics**: Collects operational metrics and provides health endpoints.
8. **Resilience Patterns**: Implements circuit breakers and retries for robust data fetching.

## Components

### Data Agent Orchestrator

The orchestrator manages two main loops:
- **Backfill Loop**: Populates missing historical data for all instruments.
- **Frontfill Loop**: Populates today's data after market close.

### Data Source Adapters

- **Polygon Adapter**: Fetches data from the Polygon API.
- **Tiingo Adapter**: Fetches data from the Tiingo API.

### Reconciliation Engine

Reconciles data from multiple sources using consensus and vendor priority strategies.

### LLM Assistant

Integrates with an LLM API to assist in:
- Source selection
- Data conflict resolution
- Anomaly detection

### MCP Server Integration

Provides a centralized interface for registering and executing tools related to the data agent.

### Monitoring & Metrics

Implements comprehensive monitoring and metrics collection:
- **Prometheus Integration**: Exports metrics for dashboard visualization
- **Health API**: Provides endpoints for health checks and metrics
- **Alerting System**: Configurable alerts for data quality and operational issues
- **Performance Metrics**: Tracks processing times, success rates, and resource usage

### Resilience Patterns

Implements robust error handling and recovery mechanisms:
- **Circuit Breakers**: Prevents cascading failures when data sources are unavailable
- **Retry Logic**: Automatically retries failed operations with exponential backoff
- **Graceful Shutdown**: Ensures clean termination of all components
- **Configurable Logging**: Structured logging with optional JSON formatting

## Usage

### Environment Variables

The following environment variables are required:

- `POLYGON_API_KEY`: API key for Polygon
- `TIINGO_API_KEY`: API key for Tiingo
- `OPENAI_API_KEY`: API key for OpenAI (for LLM assistant)

Optional environment variables for monitoring and resilience:

- `ENABLE_PROMETHEUS`: Set to "true" to enable Prometheus metrics export (default: false)
- `PROMETHEUS_PORT`: Port for Prometheus metrics server (default: 8000)
- `ENABLE_HEALTH_API`: Set to "true" to enable health API endpoints (default: false)
- `HEALTH_API_PORT`: Port for health API server (default: 8081)
- `DATA_AGENT_LOG_LEVEL`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `DATA_AGENT_LOG_FILE`: Path to log file (if not set, logs to console only)
- `DATA_AGENT_JSON_LOGS`: Set to "true" to enable JSON formatted logs (requires python-json-logger)

### Local Testing

You can run the data agent locally using the provided scripts:

#### With Real APIs

Requires all API keys to be set in environment variables:

```bash
# Run backfill process
python src/market_data/agent/run_data_agent_local.py backfill --batch-size 100 --max-iterations 1

# Run frontfill process
python src/market_data/agent/run_data_agent_local.py frontfill

# Process a specific symbol
python src/market_data/agent/run_data_agent_local.py process-symbol --symbol AAPL --start-date 2023-01-01 --end-date 2023-01-31
```

#### With Mock Adapters (No API Keys Required)

For development and testing without requiring real API keys:

```bash
# Run backfill process with mock adapters
python src/market_data/agent/run_data_agent_mock.py backfill --batch-size 100 --max-iterations 1 --use-mock-db

# Run frontfill process with mock adapters
python src/market_data/agent/run_data_agent_mock.py frontfill --use-mock-db

# Process a specific symbol with mock adapters
python src/market_data/agent/run_data_agent_mock.py process-symbol --symbol AAPL --start-date 2023-01-01 --end-date 2023-01-31 --use-mock-db
```

#### Enhanced Runner with Monitoring Features

For testing with all monitoring and operational features enabled:

```bash
# Run with monitoring, health API, and resilience features enabled
python src/market_data/agent/run_enhanced_data_agent.py backfill --batch-size 100 --max-iterations 1 --enable-monitoring --enable-prometheus --enable-health-api --json-logs

# Run with graceful shutdown handling (responds to SIGINT/SIGTERM)
python src/market_data/agent/run_enhanced_data_agent.py frontfill --enable-monitoring --log-level DEBUG
```

The enhanced runner supports all the monitoring and resilience features described above and provides a command-line interface to enable/disable specific features.

The mock runner uses:
- `MockPolygonAdapter` and `MockTiingoAdapter` that generate synthetic price data
- `MockLLMAssistant` that simulates LLM responses
- `MockDatabasePool` for simulating database interactions without a real database
- Custom JSON serialization for handling date/datetime objects

### MCP Server Integration

To use the data agent with an MCP server, initialize the `MCPToolRegistry` and register the data agent tools:

```python
from src.market_data.agent.mcp_integration import MCPToolRegistry

# Initialize registry
registry = MCPToolRegistry()

# Initialize data agent
await registry.initialize_data_agent(pool, config)

# Execute a tool
result = await registry.execute_tool("run_backfill", {"batch_size": 100, "max_iterations": 1})
```

## Testing

Unit tests and integration tests are provided for all components:

- Adapter tests: `tests/market_data/agent/test_adapters.py`
- Orchestrator tests: `tests/market_data/agent/test_orchestrator.py`
- LLM assistant tests: `tests/market_data/agent/test_llm_assistant.py`
- Integration tests: `tests/market_data/agent/test_data_agent_integration.py`

Run tests using pytest:

```bash
uv run pytest tests/market_data/agent/
```

### JSON Serialization in Tests

The integration tests use a custom `DateTimeEncoder` to handle serialization of `date` and `datetime` objects in JSON. This is important for mocking API responses and ensuring proper serialization/deserialization of date-related data.

```python
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)
```

This encoder is used to patch `json.dumps` in test fixtures to ensure proper handling of date objects.

## Development

### Adding a New Data Source

To add a new data source:

1. Create a new adapter class that implements the `VendorAdapter` interface.
2. Update the vendor priority in the reconciliation engine.
3. Register the new adapter in the data agent orchestrator.

### Extending LLM Capabilities

To extend LLM capabilities:

1. Add new methods to the `LLMAssistant` class.
2. Update the prompt templates in the LLM assistant.
3. Register new tools in the MCP tool registry.

## Deployment

For deployment to a development environment:

1. Ensure all required environment variables are set.
2. Initialize the data agent with the appropriate configuration.
3. Register the data agent tools with the MCP server.
4. Set up scheduled jobs for backfill and frontfill loops.
5. Configure monitoring dashboards using the exported Prometheus metrics.

## Monitoring & Operations

For detailed information about monitoring, metrics, alerting, and operational features, see the dedicated documentation:

[README_MONITORING.md](./README_MONITORING.md) - Comprehensive guide to monitoring and operational features

## Kubernetes & ArgoCD Deployment

For GitOps-based deployment with ArgoCD, Kubernetes manifests are available in the `k8s/data-agent/` directory. This includes:

- Data Agent deployment with monitoring enabled
- Prometheus for metrics collection
- Grafana with pre-configured dashboards
- ArgoCD Application configuration

See the [k8s/data-agent/README.md](/k8s/data-agent/README.md) for detailed deployment instructions.
