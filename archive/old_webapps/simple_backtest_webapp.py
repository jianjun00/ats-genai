#!/usr/bin/env python3
"""
Simple Backtest Results Web Application

Serves a web interface showing backtest results at the root URL.
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from datetime import date

app = FastAPI(title="Backtest Results Dashboard")

# Mock backtest data
BACKTEST_DATA = [
    {
        "id": "adaptive_sr_2024_q2",
        "name": "Adaptive Support/Resistance Strategy",
        "start_date": "2024-01-01",
        "end_date": "2024-06-30",
        "total_return": 18.47,
        "sharpe_ratio": 1.42,
        "max_drawdown": 9.23,
        "win_rate": 61.24,
        "total_trades": 143,
        "status": "completed"
    },
    {
        "id": "momentum_enhanced_2024_q2",
        "name": "Enhanced Momentum Strategy",
        "start_date": "2024-01-01", 
        "end_date": "2024-06-30",
        "total_return": 15.23,
        "sharpe_ratio": 1.18,
        "max_drawdown": 11.47,
        "win_rate": 57.89,
        "total_trades": 171,
        "status": "completed"
    },
    {
        "id": "mean_reversion_2024_q2",
        "name": "Statistical Mean Reversion",
        "start_date": "2024-01-01",
        "end_date": "2024-06-30", 
        "total_return": 8.92,
        "sharpe_ratio": 0.87,
        "max_drawdown": 6.34,
        "win_rate": 68.91,
        "total_trades": 97,
        "status": "completed"
    },
    {
        "id": "spy_benchmark_2024_q2",
        "name": "SPY Buy & Hold Benchmark",
        "start_date": "2024-01-01",
        "end_date": "2024-06-30",
        "total_return": 11.34,
        "sharpe_ratio": 0.94,
        "max_drawdown": 7.89,
        "win_rate": 52.34,
        "total_trades": 0,
        "status": "completed"
    }
]

@app.get("/", response_class=HTMLResponse)
async def backtest_dashboard():
    """Main backtest results dashboard"""
    
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Backtest Results Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh; padding: 20px; 
            }
            .container { 
                max-width: 1200px; margin: 0 auto; background: white; 
                border-radius: 12px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); 
            }
            .header { 
                background: linear-gradient(135deg, #1f77b4 0%, #1565c0 100%);
                color: white; padding: 30px; text-align: center; border-radius: 12px 12px 0 0;
            }
            .header h1 { font-size: 2.5em; margin-bottom: 10px; }
            .header p { font-size: 1.1em; opacity: 0.9; }
            .content { padding: 30px; }
            .summary { 
                display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                gap: 20px; margin-bottom: 30px; 
            }
            .summary-card { 
                background: #f8f9fa; border-radius: 8px; padding: 20px; text-align: center;
                border-left: 4px solid #1f77b4; 
            }
            .summary-value { font-size: 2em; font-weight: bold; color: #1f77b4; }
            .summary-label { font-size: 0.9em; color: #666; margin-top: 8px; }
            .backtest-grid { 
                display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); 
                gap: 20px; 
            }
            .backtest-card { 
                background: #fff; border: 1px solid #e9ecef; border-radius: 8px; 
                padding: 20px; transition: all 0.3s; cursor: pointer; 
            }
            .backtest-card:hover { 
                transform: translateY(-3px); box-shadow: 0 10px 20px rgba(0,0,0,0.1); 
                border-color: #1f77b4; 
            }
            .strategy-name { 
                font-size: 1.2em; font-weight: bold; color: #333; margin-bottom: 15px; 
            }
            .period { font-size: 0.9em; color: #666; margin-bottom: 15px; }
            .metrics { 
                display: grid; grid-template-columns: 1fr 1fr; gap: 15px; 
            }
            .metric { text-align: center; }
            .metric-label { font-size: 0.8em; color: #666; text-transform: uppercase; }
            .metric-value { font-size: 1.3em; font-weight: bold; margin-top: 5px; }
            .positive { color: #28a745; }
            .negative { color: #dc3545; }
            .neutral { color: #6c757d; }
            .status { 
                display: inline-block; padding: 4px 8px; border-radius: 4px; 
                font-size: 0.8em; font-weight: bold; text-transform: uppercase;
                background: #d4edda; color: #155724; 
            }
            .refresh-btn { 
                background: #28a745; color: white; border: none; padding: 12px 24px; 
                border-radius: 6px; cursor: pointer; margin-bottom: 20px; font-size: 1em;
            }
            .refresh-btn:hover { background: #218838; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Backtest Results Dashboard</h1>
                <p>Portfolio Strategy Performance Analysis</p>
            </div>
            
            <div class="content">
                <button class="refresh-btn" onclick="location.reload()">🔄 Refresh Results</button>
                
                <div class="summary">
                    <div class="summary-card">
                        <div class="summary-value">4</div>
                        <div class="summary-label">Total Strategies</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">18.5%</div>
                        <div class="summary-label">Best Return</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">1.42</div>
                        <div class="summary-label">Best Sharpe Ratio</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-value">411</div>
                        <div class="summary-label">Total Trades</div>
                    </div>
                </div>
                
                <div class="backtest-grid">
    """
    
    # Add backtest cards
    for bt in BACKTEST_DATA:
        return_class = "positive" if bt["total_return"] > 0 else "negative"
        
        html += f"""
                    <div class="backtest-card" onclick="showDetails('{bt["id"]}')">
                        <div class="strategy-name">{bt["name"]}</div>
                        <div class="period">{bt["start_date"]} to {bt["end_date"]}</div>
                        <div class="status">{bt["status"]}</div>
                        
                        <div class="metrics">
                            <div class="metric">
                                <div class="metric-label">Total Return</div>
                                <div class="metric-value {return_class}">{bt["total_return"]:.1f}%</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">Sharpe Ratio</div>
                                <div class="metric-value neutral">{bt["sharpe_ratio"]:.2f}</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">Max Drawdown</div>
                                <div class="metric-value negative">{bt["max_drawdown"]:.1f}%</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">Win Rate</div>
                                <div class="metric-value neutral">{bt["win_rate"]:.1f}%</div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 15px; text-align: center; color: #666;">
                            <small>Trades: {bt["total_trades"]:,}</small>
                        </div>
                    </div>
        """
    
    html += """
                </div>
            </div>
        </div>
        
        <script>
            function showDetails(backtestId) {
                alert(`Detailed view for ${backtestId} would open here.\\n\\nThis demonstrates the backtest results web interface.`);
            }
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "backtest_dashboard"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)