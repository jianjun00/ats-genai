# Polygon Instrument Data Fetcher

## Overview
The `populate_instrument_polygon.py` script fetches instrument data from the Polygon.io API and stores it in the database. It supports both single ticker and multiple ticker processing modes.

## Features
- Fetches detailed instrument data from Polygon.io API
- Supports both single ticker and batch processing with comma-separated tickers
- Uses Ray for parallel processing of batch API calls
- Implements rate limiting to avoid API throttling
- Configurable batch size and delay between API calls
- Centralized database connection with retry logic

## Usage

### Command Line Arguments
```
python -m src.secmaster.populate_instrument_polygon [OPTIONS]
```

#### Required Arguments:
None - the script can run without any arguments to process all tickers in bulk mode

#### Optional Arguments:
- `--ticker TICKER[,TICKER2,...]`: Specify one or more ticker symbols separated by commas
- `--environment ENV`: Specify environment (dev, test, prod)
- `--gin_config PATH`: Path to gin configuration file
- `--debug`: Enable debug logging
- `--bulk`: Process all tickers in bulk mode (default if no ticker specified)
- `--batch_size SIZE`: Number of tickers to process in each batch (default: 100)
- `--delay SECONDS`: Delay between API calls in seconds (default: 0.2)

### Examples

#### Process a single ticker:
```bash
python -m src.secmaster.populate_instrument_polygon --ticker AAPL --environment dev
```

#### Process multiple tickers:
```bash
python -m src.secmaster.populate_instrument_polygon --ticker AAPL,MSFT,GOOGL --environment dev
```

#### Process all tickers in bulk mode:
```bash
python -m src.secmaster.populate_instrument_polygon --bulk --environment dev
```

## Kubernetes Deployment

The script can be deployed as a Kubernetes job using the provided YAML configuration:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: populate-instrument-polygon
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: populate-instrument-polygon
        image: ats-genai:dev-latest
        command:
        - "python"
        - "-m"
        - "src.secmaster.populate_instrument_polygon"
        - "--ticker"
        - "AAPL,MSFT,GOOGL"  # Comma-separated list of tickers
        - "--environment"
        - "dev"
        - "--gin_config"
        - "config/app_docker.gin"
        env:
        - name: PYTHONPATH
          value: "/app/src"
        - name: DB_HOST
          value: "timescaledb.ats-dev.svc.cluster.local"
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          value: "postgres"
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              key: password
              name: db-credentials
        - name: DB_NAME
          value: "dev_db"
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              key: polygon-api-key
              name: api-keys
      restartPolicy: OnFailure
```

## Environment Variables

The script requires the following environment variables:
- `DB_HOST`: Database host
- `DB_PORT`: Database port
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password
- `DB_NAME`: Database name
- `POLYGON_API_KEY`: Polygon API key

## Notes
- When deploying to Kubernetes, ensure the image is available in the cluster's registry
- For large batches, consider increasing the delay to avoid API rate limiting
- The script logs detailed information about the process, including success/failure counts
