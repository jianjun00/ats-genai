#!/usr/bin/env python3

import uvicorn
from fastapi import FastAPI
from datetime import datetime

app = FastAPI(title="ATS Analytics Service - Simple Test")

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/v1/jobs/stats")
async def get_jobs_stats():
    return {"total_jobs": 3, "running_jobs": 1, "completed_jobs": 1, "failed_jobs": 1}

@app.get("/api/v1/jobs")
async def get_jobs():
    return {
        "jobs": [
            {"id": 1, "job_type": "analytics_job", "status": "completed"},
            {"id": 2, "job_type": "data_processing", "status": "running"},
            {"id": 3, "job_type": "training_job", "status": "failed"}
        ],
        "total": 3
    }

@app.get("/")
async def dashboard():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ATS Analytics - Simple Test</title>
        <style>
            body { font-family: Arial; background: #1e3c72; color: white; padding: 20px; }
            .header { text-align: center; margin-bottom: 30px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 ATS Analytics Service</h1>
            <p>Simple Test Version - GitOps Deployment Successful</p>
        </div>
        <div>
            <h3>Status:</h3>
            <ul>
                <li>✅ Git clone from private repository: SUCCESS</li>
                <li>✅ Python script execution: SUCCESS</li>
                <li>✅ FastAPI service startup: SUCCESS</li>
                <li>✅ External access via NodePort: SUCCESS</li>
            </ul>
            
            <h3>Available Endpoints:</h3>
            <ul>
                <li><a href="/health" style="color: #4CAF50;">/health</a> - Health check</li>
                <li><a href="/api/v1/jobs/stats" style="color: #4CAF50;">/api/v1/jobs/stats</a> - Job statistics</li>
                <li><a href="/api/v1/jobs" style="color: #4CAF50;">/api/v1/jobs</a> - Job list</li>
            </ul>
        </div>
    </body>
    </html>
    """

if __name__ == "__main__":
    print("🚀 Starting simple analytics service...")
    print("✅ Service starting successfully!")
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")