#!/usr/bin/env python
"""
Flyte Workflow for Instrument Polygon Operations and Enhanced Training Data Generation

This script defines a Flyte workflow for managing instrument polygon operations and 
enhanced training data generation with technical indicators.
It dynamically generates Kubernetes job configurations and can apply them to the cluster.
"""

import os
import subprocess
import tempfile
import yaml
import sys
from typing import Dict, List, Optional, Any, Tuple

import flytekit
from flytekit import task, workflow, dynamic
from flytekit.types.file import FlyteFile

# Add path for enhanced training data generator
sys.path.append('/home/jianjun/ats-genai/src')

# Import job generator utilities
try:
    from scripts.kubernetes.instrument_polygon_job_generator import (
        JobConfig,
        create_backfill_job,
        create_test_job,
    )
except ImportError:
    # Fallback for different PYTHONPATH contexts
    from scripts.kubernetes.instrument_polygon_job_generator import (
        JobConfig,
        create_backfill_job,
        create_test_job,
    )


@task
def generate_test_job_yaml(tickers: str, custom_name: str, 
                          memory_request: str, memory_limit: str,
                          cpu_request: str, cpu_limit: str) -> Tuple[str, str]:
    """
    Generate a test job YAML based on the provided parameters.
    
    Args:
        tickers: Comma-separated list of tickers
        custom_name: Custom job name
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        
    Returns:
        Tuple of (job_name, yaml_content)
    """
    # Use default tickers if not provided
    tickers_to_use = tickers if tickers else "NFLX,GOOG,AVGO,ADBE,COST"
    
    # Create the job config
    job_config = create_test_job(tickers_to_use)
    
    # Apply overrides if provided
    if custom_name:
        job_config.name = custom_name
    
    if memory_request:
        job_config.memory_request = memory_request
    
    if memory_limit:
        job_config.memory_limit = memory_limit
    
    if cpu_request:
        job_config.cpu_request = cpu_request
    
    if cpu_limit:
        job_config.cpu_limit = cpu_limit
    
    # Generate the YAML
    yaml_dict = job_config.generate_yaml()
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False)
    
    return job_config.name, yaml_content


@task
def generate_backfill_job_yaml(custom_name: str, memory_request: str, memory_limit: str,
                              cpu_request: str, cpu_limit: str) -> Tuple[str, str]:
    """
    Generate a backfill job YAML based on the provided parameters.
    
    Args:
        custom_name: Custom job name
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        
    Returns:
        Tuple of (job_name, yaml_content)
    """
    # Create the job config
    job_config = create_backfill_job()
    
    # Apply overrides if provided
    if custom_name:
        job_config.name = custom_name
    
    if memory_request:
        job_config.memory_request = memory_request
    
    if memory_limit:
        job_config.memory_limit = memory_limit
    
    if cpu_request:
        job_config.cpu_request = cpu_request
    
    if cpu_limit:
        job_config.cpu_limit = cpu_limit
    
    # Generate the YAML
    yaml_dict = job_config.generate_yaml()
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False)
    
    return job_config.name, yaml_content


@task
def save_yaml_to_file(job_name: str, yaml_content: str, output_dir: str) -> str:
    """
    Save the generated YAML to a file.
    
    Args:
        job_name: Name of the job
        yaml_content: YAML content to save
        output_dir: Directory to save the file in
        
    Returns:
        Path to the saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{job_name}.yaml")
    
    with open(output_path, 'w') as f:
        f.write(yaml_content)
    
    return output_path


@task
def apply_to_kubernetes(job_name: str, yaml_content: str) -> str:
    """
    Apply the generated YAML to the Kubernetes cluster.
    
    Args:
        job_name: Name of the job
        yaml_content: YAML content to apply
        
    Returns:
        Result message
    """
    # Create a temporary file to store the YAML
    with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
        tmp.write(yaml_content.encode('utf-8'))
        tmp_path = tmp.name
    
    try:
        # Apply the job to the cluster
        result = subprocess.run(
            ['kubectl', 'apply', '-f', tmp_path],
            capture_output=True,
            text=True,
            check=True
        )
        return f"Successfully applied job {job_name}: {result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"Failed to apply job {job_name}: {e.stderr}"
    finally:
        # Clean up the temporary file
        os.unlink(tmp_path)


@task
def enhanced_training_data_task(
    symbol: str = "AAPL",
    days_back: int = 90,
    sequence_length: int = 21
) -> Dict[str, Any]:
    """
    Enhanced training data generation task with technical indicators.
    
    This integrates with existing Flyte infrastructure to generate:
    - OHLC sequences for past 21 bars
    - Technical indicators: etop, ebot, pldot, oneonedot
    - Feature distributions for visualization
    - Database integration with metadata storage
    
    Args:
        symbol: Stock symbol to generate data for
        days_back: Number of days of historical data  
        sequence_length: Length of sequences (21 bars for past 21 bars)
    
    Returns:
        Dictionary with generation results and metadata
    """
    
    print(f"🚀 Starting Enhanced Training Data Generation for {symbol}")
    print(f"Parameters: days_back={days_back}, sequence_length={sequence_length}")
    
    try:
        # Import enhanced training data generator
        import asyncio
        from src.app.enhanced_training_data_generator import run_enhanced_training_data_job_for_symbol
        
        # Run the enhanced training data generation
        async def run_generation():
            return await run_enhanced_training_data_job_for_symbol(
                symbol=symbol, 
                days_back=days_back
            )
        
        # Execute in async context
        results = asyncio.run(run_generation())
        
        print(f"✅ Enhanced training data generation results:")
        print(f"  Status: {results['status']}")
        
        if results['status'] == 'success':
            print(f"  Run ID: {results['run_id']}")
            print(f"  Dataset IDs: {results['dataset_ids']}")
            print(f"  Features Shape: {results['features_shape']}")
            print(f"  Labels Shape: {results['labels_shape']}")
            
            # Extract metadata for logging
            metadata = results.get('metadata', {})
            if 'feature_names' in metadata:
                print(f"  Feature Names: {metadata['feature_names']}")
            if 'technical_indicators' in metadata:
                indicators = list(metadata['technical_indicators'].keys())
                print(f"  Technical Indicators: {indicators}")
                
            print("\n🎉 Enhanced features generated successfully:")
            print("  • OHLC sequences (21 bars)")
            print("  • Elliott Top (etop) - reversal indicator")
            print("  • Elliott Bottom (ebot) - reversal indicator")
            print("  • Pivot Line Dot (pldot) - momentum indicator") 
            print("  • One-One-Dot (oneonedot) - custom oscillator")
            print("  • Feature distributions for visualization")
            print("  • Database metadata storage")
        else:
            print(f"  Error: {results.get('error', 'Unknown error')}")
        
        return results
        
    except Exception as e:
        error_msg = f"Enhanced training data generation failed: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        
        return {
            'status': 'error',
            'error': error_msg,
            'symbol': symbol,
            'days_back': days_back
        }


@task
def format_result(output_path: str, apply_result: str = "") -> str:
    """
    Format the result message.
    
    Args:
        output_path: Path where the YAML was saved
        apply_result: Result of applying the job to the cluster (if any)
        
    Returns:
        Formatted result message
    """
    if apply_result:
        return f"Job saved to {output_path} and applied to cluster: {apply_result}"
    else:
        return f"Job saved to {output_path}"


@dynamic
def dynamic_job_workflow(
    job_type: str,
    tickers: str = "",
    memory_request: str = "",
    memory_limit: str = "",
    cpu_request: str = "",
    cpu_limit: str = "",
    custom_name: str = "",
    should_apply: bool = False,
    output_dir: str = "/home/jianjun/ats-genai/k8s/generated"
) -> str:
    """
    Dynamic workflow that handles job type selection at runtime.
    
    Args:
        job_type: Type of job (backfill or test)
        tickers: Comma-separated list of tickers (for test job)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        custom_name: Custom job name
        should_apply: Whether to apply the job to the cluster
        output_dir: Directory to save the generated YAML
        
    Returns:
        Result message
    """
    # Generate job YAML based on job type
    if job_type == "test":
        job_name, yaml_content = generate_test_job_yaml(
            tickers=tickers,
            custom_name=custom_name,
            memory_request=memory_request,
            memory_limit=memory_limit,
            cpu_request=cpu_request,
            cpu_limit=cpu_limit
        )
    else:  # backfill
        job_name, yaml_content = generate_backfill_job_yaml(
            custom_name=custom_name,
            memory_request=memory_request,
            memory_limit=memory_limit,
            cpu_request=cpu_request,
            cpu_limit=cpu_limit
        )
    
    # Save the YAML to a file
    output_path = save_yaml_to_file(
        job_name=job_name,
        yaml_content=yaml_content,
        output_dir=output_dir
    )
    
    # Apply to cluster if requested
    if should_apply:
        apply_result = apply_to_kubernetes(
            job_name=job_name,
            yaml_content=yaml_content
        )
        return format_result(output_path=output_path, apply_result=apply_result)
    else:
        return format_result(output_path=output_path)


@workflow
def enhanced_training_data_workflow(
    symbol: str = "AAPL",
    days_back: int = 90,
    sequence_length: int = 21
) -> Dict[str, Any]:
    """
    Enhanced training data generation workflow integrated with existing Flyte infrastructure.
    
    This workflow generates comprehensive training data with:
    1. OHLC price sequences for the past 21 bars
    2. Technical indicators (etop, ebot, pldot, oneonedot)
    3. Feature distributions for web app visualization
    4. Database storage with enhanced metadata
    
    Args:
        symbol: Stock symbol (default: AAPL)
        days_back: Historical data period (default: 90 days)
        sequence_length: Sequence length (default: 21 bars)
    
    Returns:
        Training data generation results with metadata
    """
    
    print(f"📊 Enhanced Training Data Workflow")
    print(f"Symbol: {symbol}")
    print(f"Period: {days_back} days")
    print(f"Sequence Length: {sequence_length} bars")
    print()
    print("Features to be generated:")
    print("  • OHLC (Open, High, Low, Close)")
    print("  • Volume")
    print("  • Elliott Top (etop) - 21 periods")
    print("  • Elliott Bottom (ebot) - 21 periods")
    print("  • Pivot Line Dot (pldot) - 21 periods")
    print("  • One-One-Dot (oneonedot) - 21 periods")
    print()
    
    # Execute enhanced training data generation task
    results = enhanced_training_data_task(
        symbol=symbol,
        days_back=days_back,
        sequence_length=sequence_length
    )
    
    return results


@workflow
def instrument_polygon_workflow(
    job_type: str,
    tickers: str = "",
    memory_request: str = "",
    memory_limit: str = "",
    cpu_request: str = "",
    cpu_limit: str = "",
    custom_name: str = "",
    should_apply: bool = False,
    output_dir: str = "/home/jianjun/ats-genai/k8s/generated"
) -> str:
    """
    Main workflow for instrument polygon operations.
    
    Args:
        job_type: Type of job (backfill or test)
        tickers: Comma-separated list of tickers (for test job)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        custom_name: Custom job name
        should_apply: Whether to apply the job to the cluster
        output_dir: Directory to save the generated YAML
        
    Returns:
        Result message
    """
    return dynamic_job_workflow(
        job_type=job_type,
        tickers=tickers,
        memory_request=memory_request,
        memory_limit=memory_limit,
        cpu_request=cpu_request,
        cpu_limit=cpu_limit,
        custom_name=custom_name,
        should_apply=should_apply,
        output_dir=output_dir
    )


# Note: create_backfill_job is imported from scripts.kubernetes.k8s_job_generator


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Flyte workflows: instrument polygon operations or enhanced training data generation")
    subparsers = parser.add_subparsers(dest='workflow_type', help='Type of workflow to run')
    
    # Instrument polygon workflow
    polygon_parser = subparsers.add_parser('polygon', help='Run instrument polygon workflow')
    polygon_parser.add_argument('--job-type', choices=['backfill', 'test'], required=True, 
                        help='Type of job to generate')
    polygon_parser.add_argument('--tickers', type=str, default="",
                        help='Comma-separated list of tickers (for test job only)')
    polygon_parser.add_argument('--memory-request', type=str, default="",
                        help='Memory request (e.g., 256Mi)')
    polygon_parser.add_argument('--memory-limit', type=str, default="",
                        help='Memory limit (e.g., 512Mi)')
    polygon_parser.add_argument('--cpu-request', type=str, default="",
                        help='CPU request (e.g., 100m)')
    polygon_parser.add_argument('--cpu-limit', type=str, default="",
                        help='CPU limit (e.g., 250m)')
    polygon_parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')
    polygon_parser.add_argument('--custom-name', type=str, default="",
                        help='Custom job name')
    polygon_parser.add_argument('--apply', action='store_true',
                        help='Apply the job to the cluster')
    polygon_parser.add_argument('--output-dir', type=str, default="/home/jianjun/ats-genai/k8s/generated",
                        help='Directory to save the generated YAML')
    
    # Enhanced training data workflow
    training_parser = subparsers.add_parser('training', help='Run enhanced training data generation workflow')
    training_parser.add_argument('--symbol', type=str, default="AAPL",
                        help='Stock symbol to generate training data for (default: AAPL)')
    training_parser.add_argument('--days-back', type=int, default=90,
                        help='Number of days of historical data (default: 90)')
    training_parser.add_argument('--sequence-length', type=int, default=21,
                        help='Sequence length for past bars (default: 21)')
    
    args = parser.parse_args()
    
    # Run the appropriate workflow
    if args.workflow_type == 'polygon':
        print("🔧 Running Instrument Polygon Workflow...")
        result = instrument_polygon_workflow(
            job_type=args.job_type,
            tickers=args.tickers,
            memory_request=args.memory_request,
            memory_limit=args.memory_limit,
            cpu_request=args.cpu_request,
            cpu_limit=args.cpu_limit,
            custom_name=args.custom_name,
            should_apply=args.apply,
            output_dir=args.output_dir
        )
        print(result)
        
    elif args.workflow_type == 'training':
        print("📊 Running Enhanced Training Data Generation Workflow...")
        result = enhanced_training_data_workflow(
            symbol=args.symbol,
            days_back=args.days_back,
            sequence_length=args.sequence_length
        )
        print(f"Enhanced Training Data Result: {result}")
        
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  # Run enhanced training data generation for AAPL")
        print("  python flyte_instrument_polygon_workflow.py training --symbol AAPL --days-back 90")
        print()
        print("  # Run instrument polygon backfill job")
        print("  python flyte_instrument_polygon_workflow.py polygon --job-type backfill --apply")
