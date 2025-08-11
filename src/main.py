from fastapi import FastAPI
from events.api import router as events_router

app = FastAPI(title="ATS GenAI API", description="Algorithmic Trading System with GenAI")

# Include the events router with a prefix
app.include_router(events_router, prefix="/api/v1", tags=["events"])

@app.get("/")
async def root():
    return {"message": "ATS GenAI API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
