import os
import asyncio
from fastapi import FastAPI, BackgroundTasks
from market_data import MarketDataStreamer
from signals import compute_signals
from model import load_model, predict_return
from typing import Dict

app = FastAPI()

# Shared state
latest_forecast = {"forecast": None, "timestamp": None}

@app.on_event("startup")
async def startup_event():
    """Start background market data streaming and forecasting."""
    loop = asyncio.get_event_loop()
    loop.create_task(background_data_task())

async def background_data_task():
    model = load_model()
    async for bar in MarketDataStreamer(symbols=["AAPL"]).stream_bars():
        signals = compute_signals(bar)
        forecast = predict_return(model, signals)
        latest_forecast["forecast"] = forecast
        latest_forecast["timestamp"] = bar["timestamp"]

@app.get("/forecast")
def get_forecast() -> Dict:
    return latest_forecast

@app.get("/health")
def health():
    return {"status": "ok"}
