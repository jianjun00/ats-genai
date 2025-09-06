#!/usr/bin/env python3
"""
Local Model Server

FastAPI server for hosting self-hosted LLM models with:
- REST API endpoints compatible with OpenAI format
- Health monitoring and metrics
- GPU resource management
- Batch processing support
- Performance optimization
"""

import asyncio
import argparse
import logging
import json
import time
import uvicorn
from typing import Dict, Any, List, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from infrastructure.llm.local_model_client import (
    LocalModelClient, LocalModelConfig, LocalModelMetrics,
    create_fingpt_config, create_llama_8b_config, create_llama_70b_config
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global model client
model_client: Optional[LocalModelClient] = None


# Pydantic models for API
class ChatMessage(BaseModel):
    role: str = Field(..., description="Role of the message sender")
    content: str = Field(..., description="Content of the message")


class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage] = Field(..., description="List of messages")
    model: Optional[str] = Field(None, description="Model to use")
    max_tokens: Optional[int] = Field(512, description="Maximum tokens to generate")
    temperature: Optional[float] = Field(0.1, description="Temperature for generation")
    top_p: Optional[float] = Field(0.9, description="Top-p for generation")
    stream: Optional[bool] = Field(False, description="Stream response")


class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[Dict[str, Any]]
    usage: Dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    gpu_available: bool
    memory_usage: Dict[str, Any]
    performance_metrics: Dict[str, Any]
    timestamp: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager for model initialization."""
    global model_client
    
    # Startup
    logger.info("Starting model server...")
    try:
        # Get model configuration from environment/args
        model_config = get_model_config()
        model_client = LocalModelClient(model_config)
        await model_client.initialize()
        logger.info("Model loaded successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize model: {e}")
        raise
    finally:
        # Shutdown
        if model_client:
            await model_client.close()
        logger.info("Model server shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="ATS Local LLM Server",
    description="Self-hosted LLM models for financial news analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_model_config() -> LocalModelConfig:
    """Get model configuration from environment variables."""
    import os
    
    model_type = os.getenv('MODEL_TYPE', 'llama')
    model_id = os.getenv('MODEL_ID', 'meta-llama/Meta-Llama-3.1-8B-Instruct')
    precision = os.getenv('PRECISION', 'fp16')
    device = os.getenv('DEVICE', 'auto')
    max_length = int(os.getenv('MAX_LENGTH', '2048'))
    enable_quantization = os.getenv('ENABLE_QUANTIZATION', 'true').lower() == 'true'
    
    if model_type == 'fingpt':
        config = create_fingpt_config(precision, enable_quantization)
    elif model_type == 'llama' and '8B' in model_id:
        config = create_llama_8b_config(precision, enable_quantization)
    elif model_type == 'llama' and '70B' in model_id:
        config = create_llama_70b_config(precision, enable_quantization)
    else:
        # Generic configuration
        config = LocalModelConfig(
            model_id=model_id,
            model_type=model_type,
            precision=precision,
            device=device,
            max_length=max_length,
            enable_quantization=enable_quantization
        )
    
    # Override with environment variables if provided
    if os.getenv('BASE_MODEL'):
        config.base_model = os.getenv('BASE_MODEL')
    if os.getenv('PEFT_MODEL'):
        config.peft_model = os.getenv('PEFT_MODEL')
    
    return config


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    global model_client
    
    if not model_client:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        import torch
        import GPUtil
        
        # Check model health
        is_healthy = await model_client.health_check()
        
        # GPU information
        gpu_available = torch.cuda.is_available()
        gpu_info = {}
        if gpu_available:
            gpus = GPUtil.getGPUs()
            if gpus:
                gpu = gpus[0]
                gpu_info = {
                    "gpu_name": gpu.name,
                    "memory_used_mb": gpu.memoryUsed,
                    "memory_total_mb": gpu.memoryTotal,
                    "utilization_percent": gpu.load * 100
                }
        
        # Performance metrics
        metrics = model_client.get_performance_metrics()
        
        return HealthResponse(
            status="healthy" if is_healthy else "unhealthy",
            model_loaded=model_client._initialized,
            gpu_available=gpu_available,
            memory_usage=gpu_info,
            performance_metrics=metrics,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@app.post("/v1/chat/completions", response_model=ChatCompletionResponse)
async def chat_completions(request: ChatCompletionRequest):
    """OpenAI-compatible chat completions endpoint."""
    global model_client
    
    if not model_client:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Convert messages to prompt
        prompt = format_messages_as_prompt(request.messages)
        
        # Generate response
        response = await model_client.generate_response(
            prompt,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            top_p=request.top_p
        )
        
        # Format as OpenAI response
        return ChatCompletionResponse(
            id=f"chatcmpl-{int(time.time())}",
            created=int(time.time()),
            model=model_client.config.model_id,
            choices=[{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": response.content
                },
                "finish_reason": "stop"
            }],
            usage={
                "prompt_tokens": response.tokens_used - len(response.content.split()),
                "completion_tokens": len(response.content.split()),
                "total_tokens": response.tokens_used,
                "estimated_cost_usd": response.cost_usd,
                "latency_ms": response.latency_ms
            }
        )
        
    except Exception as e:
        logger.error(f"Chat completion failed: {e}")
        raise HTTPException(status_code=500, detail=f"Generation failed: {str(e)}")


@app.post("/v1/completions")
async def completions(
    prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.1,
    top_p: float = 0.9
):
    """Simple completions endpoint."""
    global model_client
    
    if not model_client:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        response = await model_client.generate_response(
            prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p
        )
        
        return {
            "id": f"cmpl-{int(time.time())}",
            "object": "text_completion",
            "created": int(time.time()),
            "model": model_client.config.model_id,
            "choices": [{
                "text": response.content,
                "index": 0,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": response.tokens_used - len(response.content.split()),
                "completion_tokens": len(response.content.split()),
                "total_tokens": response.tokens_used
            }
        }
        
    except Exception as e:
        logger.error(f"Completion failed: {e}")
        raise HTTPException(status_code=500, detail=f"Generation failed: {str(e)}")


@app.get("/metrics")
async def get_metrics():
    """Prometheus-style metrics endpoint."""
    global model_client
    
    if not model_client:
        return {"error": "Model not loaded"}
    
    metrics = model_client.get_performance_metrics()
    
    # Convert to Prometheus format
    prometheus_metrics = []
    
    if metrics.get('inference_count', 0) > 0:
        prometheus_metrics.extend([
            f"# HELP model_inference_total Total number of inferences",
            f"# TYPE model_inference_total counter",
            f"model_inference_total {metrics['inference_count']}",
            f"",
            f"# HELP model_tokens_generated_total Total tokens generated",
            f"# TYPE model_tokens_generated_total counter", 
            f"model_tokens_generated_total {metrics['total_tokens_generated']}",
            f"",
            f"# HELP model_inference_duration_seconds Average inference duration",
            f"# TYPE model_inference_duration_seconds gauge",
            f"model_inference_duration_seconds {metrics['avg_inference_time_seconds']}",
            f"",
            f"# HELP model_tokens_per_second Token generation rate",
            f"# TYPE model_tokens_per_second gauge",
            f"model_tokens_per_second {metrics['tokens_per_second']}",
            f""
        ])
    
    if 'gpu_memory_used_mb' in metrics:
        prometheus_metrics.extend([
            f"# HELP gpu_memory_used_bytes GPU memory used",
            f"# TYPE gpu_memory_used_bytes gauge",
            f"gpu_memory_used_bytes {metrics['gpu_memory_used_mb'] * 1024 * 1024}",
            f"",
            f"# HELP gpu_utilization_percent GPU utilization percentage", 
            f"# TYPE gpu_utilization_percent gauge",
            f"gpu_utilization_percent {metrics['gpu_utilization_percent']}",
            f""
        ])
    
    return {"metrics": "\n".join(prometheus_metrics)}


@app.get("/model/info")
async def model_info():
    """Get model information."""
    global model_client
    
    if not model_client:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    return {
        "model_id": model_client.config.model_id,
        "model_type": model_client.config.model_type,
        "device": str(model_client.device),
        "precision": model_client.config.precision,
        "max_length": model_client.config.max_length,
        "quantization_enabled": model_client.config.enable_quantization,
        "initialized": model_client._initialized
    }


def format_messages_as_prompt(messages: List[ChatMessage]) -> str:
    """Convert chat messages to a prompt format."""
    prompt_parts = []
    
    for message in messages:
        role = message.role
        content = message.content
        
        if role == "system":
            prompt_parts.append(f"System: {content}")
        elif role == "user":
            prompt_parts.append(f"User: {content}")
        elif role == "assistant":
            prompt_parts.append(f"Assistant: {content}")
    
    # Add final assistant prompt
    prompt_parts.append("Assistant:")
    
    return "\n\n".join(prompt_parts)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Local LLM Model Server")
    parser.add_argument("--model-type", default="llama", choices=["fingpt", "llama", "custom"])
    parser.add_argument("--model-id", default="meta-llama/Meta-Llama-3.1-8B-Instruct")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--workers", type=int, default=1)
    
    args = parser.parse_args()
    
    # Set environment variables for configuration
    import os
    os.environ['MODEL_TYPE'] = args.model_type
    os.environ['MODEL_ID'] = args.model_id
    
    # Start server
    logger.info(f"Starting model server on {args.host}:{args.port}")
    logger.info(f"Model type: {args.model_type}, Model ID: {args.model_id}")
    
    uvicorn.run(
        "infrastructure.llm.model_server:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_level="info"
    )


if __name__ == "__main__":
    main()