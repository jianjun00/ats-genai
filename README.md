# Market Forecast App

A server-side application that connects to Alpaca for real-time market data, computes technical signals, and uses a PyTorch deep learning time series model to forecast next hourly or daily returns.

## Features
- Real-time market data subscription (Alpaca)
- Technical indicator computation (pandas_ta)
- Deep learning time series forecasting (PyTorch LSTM)
- REST API (FastAPI)
- Kubernetes-ready deployment for GCP

## Setup

1. **Clone the repo**
2. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3. **Set Alpaca API keys:**
    - Set environment variables `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` (or use Kubernetes secrets)
4. **Run the server:**
    ```bash
    uvicorn main:app --reload
    ```
5. **Build & deploy with Docker/Kubernetes:**
    See `Dockerfile` and `k8s/` directory for manifests.

## API Endpoints
- `GET /forecast` — Get the latest forecast
- `GET /health` — Health check

## Notes
- This is a demo. For production, add error handling, persistent storage, and robust model retraining.
