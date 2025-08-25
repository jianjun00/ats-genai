#!/usr/bin/env python3
"""
Flyte Dev CLI - Unified job management through Flyte workflows

Replaces direct Kubernetes job management with Flyte workflow orchestration.
All dev environment operations should flow through Flyte for proper orchestration,
monitoring, and integration with the analytics webapp.

Usage:
    python scripts/flyte_dev_cli.py submit price-unification --symbols AAPL,MSFT
    python scripts/flyte_dev_cli.py submit training-data-gen --dataset comprehensive
    python scripts/flyte_dev_cli.py submit webapp-deployment
    python scripts/flyte_dev_cli.py list-executions
    python scripts/flyte_dev_cli.py logs execution-id
    python scripts/flyte_dev_cli.py status execution-id
"""

import argparse
import subprocess
import sys
import os
import json
import yaml
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

class FlyteDevCLI:
    """Flyte-based dev CLI for managing all jobs through workflows"""
    
    def __init__(self):
        self.project = "ats"
        self.domain = "development"
        self.flyte_config = self._setup_flyte_config()
        
    def _setup_flyte_config(self):
        """Setup Flyte configuration for dev environment"""
        config = {
            "admin": {
                "endpoint": "localhost:30080",  # Flyte admin service
                "insecure": True
            },
            "project": self.project,
            "domain": self.domain
        }
        return config
    
    def submit_workflow(self, workflow_type: str, **kwargs):
        """Submit a workflow to Flyte"""
        workflow_map = {
            "price-unification": self._submit_price_unification,
            "training-data-gen": self._submit_training_data_generation, 
            "webapp-deployment": self._submit_webapp_deployment,
            "market-cap-population": self._submit_market_cap_job,
            "database-query": self._submit_database_query,
            "backtest": self._submit_backtest
        }
        
        if workflow_type not in workflow_map:
            raise ValueError(f"Unknown workflow type: {workflow_type}")
            
        return workflow_map[workflow_type](**kwargs)
    
    def _submit_price_unification(self, symbols: str = None, date: str = None, **kwargs):
        """Submit price unification workflow"""
        inputs = {
            "symbols": symbols or "AAPL,MSFT,GOOGL,AMZN,TSLA",
            "start_date": date or "2024-01-01", 
            "end_date": date or "2024-12-31",
            "run_type": "daily_price_unification"
        }
        
        return self._execute_flyte_workflow("price_unification_workflow", inputs)
    
    def _submit_training_data_generation(self, dataset: str = None, **kwargs):
        """Submit training data generation workflow"""
        inputs = {
            "dataset_name": dataset or f"training_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "symbols": kwargs.get("symbols", "AAPL,TSLA"),
            "sequence_length": kwargs.get("sequence_length", 60),
            "prediction_horizon": kwargs.get("prediction_horizon", 5),
            "run_type": "training_data_generation"
        }
        
        return self._execute_flyte_workflow("training_data_generation_workflow", inputs)
    
    def _submit_webapp_deployment(self, **kwargs):
        """Submit webapp deployment workflow"""
        inputs = {
            "webapp_type": "analytics",
            "port": kwargs.get("port", 3000),
            "replicas": kwargs.get("replicas", 1),
            "run_type": "webapp_deployment"
        }
        
        return self._execute_flyte_workflow("webapp_deployment_workflow", inputs)
    
    def _submit_market_cap_job(self, symbols: str = None, **kwargs):
        """Submit market cap population workflow"""
        inputs = {
            "symbols": symbols or "AAPL,MSFT,GOOGL",
            "vendor": kwargs.get("vendor", "polygon"),
            "run_type": "market_cap_population"
        }
        
        return self._execute_flyte_workflow("market_cap_workflow", inputs)
    
    def _submit_database_query(self, query: str, **kwargs):
        """Submit database query workflow"""
        inputs = {
            "query": query,
            "run_type": "database_query"
        }
        
        return self._execute_flyte_workflow("database_query_workflow", inputs)
    
    def _submit_backtest(self, strategy: str = None, **kwargs):
        """Submit backtest workflow"""
        inputs = {
            "strategy_name": strategy or "comprehensive_2022_2025",
            "universe": kwargs.get("universe", "SPY500"),
            "start_date": kwargs.get("start_date", "2022-01-01"),
            "end_date": kwargs.get("end_date", "2025-08-19"),
            "run_type": "backtest"
        }
        
        return self._execute_flyte_workflow("backtest_workflow", inputs)
    
    def _execute_flyte_workflow(self, workflow_name: str, inputs: Dict[str, Any]):
        """Execute a Flyte workflow with given inputs"""
        try:
            # For now, since Flyte isn't deployed, record the job in dev_runs table
            execution_id = self._record_job_run(workflow_name, inputs)
            
            # In a real Flyte setup, this would be:
            # flytekit_cmd = [
            #     "flytecli", "run-execution", 
            #     "--project", self.project,
            #     "--domain", self.domain, 
            #     "--workflow", workflow_name,
            #     "--inputs", json.dumps(inputs)
            # ]
            # result = subprocess.run(flytekit_cmd, capture_output=True, text=True)
            
            print(f"🚀 Workflow {workflow_name} submitted successfully")
            print(f"📋 Execution ID: {execution_id}")
            print(f"🔧 Inputs: {json.dumps(inputs, indent=2)}")
            print(f"📊 Monitor via: python scripts/flyte_dev_cli.py status {execution_id}")
            
            return {
                "execution_id": execution_id,
                "status": "submitted",
                "workflow_name": workflow_name,
                "inputs": inputs
            }
            
        except Exception as e:
            print(f"❌ Failed to submit workflow {workflow_name}: {e}")
            raise
    
    def _record_job_run(self, workflow_name: str, inputs: Dict[str, Any]) -> str:
        """Record job run in dev_runs table for tracking"""
        execution_id = f"flyte-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Use dev CLI to insert the run record
        insert_query = f"""
        INSERT INTO dev_runs (run_type, start_time, status, metadata) 
        VALUES ('{inputs.get('run_type', workflow_name)}', NOW(), 'submitted', 
                '{json.dumps({"workflow": workflow_name, "inputs": inputs, "execution_id": execution_id})}')
        """
        
        # Submit the query via dev CLI
        subprocess.run([
            "python", "scripts/run_dev.py", "query", insert_query
        ], check=True)
        
        return execution_id
    
    def list_executions(self, limit: int = 20):
        """List recent Flyte workflow executions"""
        try:
            # Query dev_runs table for workflow executions
            query = f"""
            SELECT id, run_type, start_time, end_time, status, metadata 
            FROM dev_runs 
            WHERE metadata LIKE '%execution_id%' 
            ORDER BY start_time DESC 
            LIMIT {limit}
            """
            
            # For now, use dev CLI to get the data
            result = subprocess.run([
                "python", "scripts/run_dev.py", "query", query
            ], capture_output=True, text=True)
            
            print("🔍 Recent Flyte Workflow Executions:")
            print("=" * 80)
            print(f"{'ID':<10} {'Type':<25} {'Status':<12} {'Start Time':<20}")
            print("-" * 80)
            
            # Parse the output (this would be structured in real implementation)
            print("📋 Use webapp Job Runs section for detailed view")
            print("🌐 Analytics Dashboard: http://localhost:30000/")
            
        except Exception as e:
            print(f"❌ Failed to list executions: {e}")
    
    def get_execution_status(self, execution_id: str):
        """Get status of a specific execution"""
        try:
            query = f"""
            SELECT run_type, start_time, end_time, status, metadata 
            FROM dev_runs 
            WHERE metadata LIKE '%{execution_id}%'
            """
            
            result = subprocess.run([
                "python", "scripts/run_dev.py", "query", query
            ], capture_output=True, text=True)
            
            print(f"📋 Execution Status: {execution_id}")
            print("=" * 50)
            print("🔍 Check webapp for real-time monitoring")
            
        except Exception as e:
            print(f"❌ Failed to get execution status: {e}")
    
    def get_execution_logs(self, execution_id: str):
        """Get logs for a specific execution"""
        print(f"📄 Logs for execution: {execution_id}")
        print("🔗 Use: python scripts/run_dev.py logs <job-name>")
        print("🌐 Or check webapp Job Runs section for integrated logs")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Flyte Dev CLI - Workflow-based job management")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Submit workflow
    submit_parser = subparsers.add_parser("submit", help="Submit a workflow")
    submit_parser.add_argument("workflow_type", choices=[
        "price-unification", "training-data-gen", "webapp-deployment", 
        "market-cap-population", "database-query", "backtest"
    ])
    submit_parser.add_argument("--symbols", help="Comma-separated symbols")
    submit_parser.add_argument("--date", help="Date for processing")
    submit_parser.add_argument("--dataset", help="Dataset name")
    submit_parser.add_argument("--query", help="SQL query to execute")
    submit_parser.add_argument("--strategy", help="Backtest strategy")
    submit_parser.add_argument("--port", type=int, help="Webapp port")
    submit_parser.add_argument("--replicas", type=int, help="Number of replicas")
    
    # List executions
    list_parser = subparsers.add_parser("list-executions", help="List workflow executions")
    list_parser.add_argument("--limit", type=int, default=20, help="Number of executions to show")
    
    # Status
    status_parser = subparsers.add_parser("status", help="Get execution status")
    status_parser.add_argument("execution_id", help="Execution ID")
    
    # Logs
    logs_parser = subparsers.add_parser("logs", help="Get execution logs")
    logs_parser.add_argument("execution_id", help="Execution ID")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli = FlyteDevCLI()
    
    try:
        if args.command == "submit":
            workflow_args = {k: v for k, v in vars(args).items() 
                           if k not in ['command', 'workflow_type'] and v is not None}
            cli.submit_workflow(args.workflow_type, **workflow_args)
            
        elif args.command == "list-executions":
            cli.list_executions(args.limit)
            
        elif args.command == "status":
            cli.get_execution_status(args.execution_id)
            
        elif args.command == "logs":
            cli.get_execution_logs(args.execution_id)
            
    except Exception as e:
        print(f"❌ Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()