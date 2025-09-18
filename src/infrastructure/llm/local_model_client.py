#!/usr/bin/env python3
"""
Local Model Client for Self-Hosted LLM Deployment

This module provides a comprehensive client for running self-hosted LLM models
with support for multiple model types, GPU optimization, and financial analysis.

Supported Models:
- FinGPT v3 series (specialized for financial sentiment analysis)
- Llama 3.1 8B/70B (general purpose with financial fine-tuning)
- Custom fine-tuned models

Features:
- GPU optimization with automatic device management
- Model quantization for memory efficiency
- Batched inference for high throughput
- Health monitoring and performance metrics
- Integration with existing multi-provider client
"""

import asyncio
import logging
import time
import psutil
import GPUtil
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
import torch
from transformers import (
    AutoTokenizer, AutoModelForCausalLM,
    BitsAndBytesConfig
)
from peft import PeftModel
import numpy as np

from src.infrastructure.llm.multi_provider_client import LLMResponse

logger = logging.getLogger(__name__)


@dataclass
class LocalModelConfig:
    """Configuration for local model deployment."""
    model_id: str
    model_type: str  # 'fingpt', 'llama', 'custom'
    base_model: Optional[str] = None  # For PEFT models
    peft_model: Optional[str] = None  # For LoRA/PEFT models
    device: str = "auto"  # 'auto', 'cuda:0', 'cpu'
    precision: str = "fp16"  # 'fp32', 'fp16', 'int8', 'int4'
    max_length: int = 2048
    temperature: float = 0.1
    top_p: float = 0.9
    batch_size: int = 1
    enable_quantization: bool = True
    trust_remote_code: bool = True


class LocalModelMetrics:
    """Performance and resource usage metrics for local models."""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset all metrics."""
        self.inference_count = 0
        self.total_inference_time = 0.0
        self.total_tokens_generated = 0
        self.gpu_memory_samples = []
        self.cpu_usage_samples = []
        self.error_count = 0
        self.last_inference_time = None

    def record_inference(self, duration: float, tokens_generated: int):
        """Record inference metrics."""
        self.inference_count += 1
        self.total_inference_time += duration
        self.total_tokens_generated += tokens_generated
        self.last_inference_time = datetime.now()

        # Sample system metrics
        self._sample_system_metrics()

    def record_error(self):
        """Record inference error."""
        self.error_count += 1

    def _sample_system_metrics(self):
        """Sample GPU and CPU metrics."""
        try:
            # GPU metrics
            gpus = GPUtil.getGPUs()
            if gpus:
                gpu = gpus[0]
                self.gpu_memory_samples.append({
                    'used_mb': gpu.memoryUsed,
                    'total_mb': gpu.memoryTotal,
                    'utilization': gpu.load * 100,
                    'timestamp': time.time()
                })

            # CPU metrics
            cpu_percent = psutil.cpu_percent()
            self.cpu_usage_samples.append({
                'usage_percent': cpu_percent,
                'timestamp': time.time()
            })

            # Keep only last 100 samples
            if len(self.gpu_memory_samples) > 100:
                self.gpu_memory_samples = self.gpu_memory_samples[-100:]
            if len(self.cpu_usage_samples) > 100:
                self.cpu_usage_samples = self.cpu_usage_samples[-100:]

        except Exception as e:
            logger.warning(f"Failed to sample system metrics: {e}")

    def get_summary(self) -> Dict[str, Any]:
        """Get performance metrics summary."""
        if self.inference_count == 0:
            return {'status': 'no_inferences_yet'}

        avg_inference_time = self.total_inference_time / self.inference_count
        tokens_per_second = self.total_tokens_generated / self.total_inference_time if self.total_inference_time > 0 else 0

        summary = {
            'inference_count': self.inference_count,
            'total_tokens_generated': self.total_tokens_generated,
            'avg_inference_time_seconds': avg_inference_time,
            'tokens_per_second': tokens_per_second,
            'error_count': self.error_count,
            'error_rate': self.error_count / self.inference_count,
            'last_inference_time': self.last_inference_time.isoformat() if self.last_inference_time else None
        }

        # GPU metrics
        if self.gpu_memory_samples:
            latest_gpu = self.gpu_memory_samples[-1]
            avg_gpu_usage = np.mean([s['utilization'] for s in self.gpu_memory_samples[-10:]])
            max_gpu_memory = max([s['used_mb'] for s in self.gpu_memory_samples])

            summary.update({
                'gpu_memory_used_mb': latest_gpu['used_mb'],
                'gpu_memory_total_mb': latest_gpu['total_mb'],
                'gpu_utilization_percent': latest_gpu['utilization'],
                'avg_gpu_utilization_percent': avg_gpu_usage,
                'max_gpu_memory_used_mb': max_gpu_memory
            })

        # CPU metrics
        if self.cpu_usage_samples:
            avg_cpu_usage = np.mean([s['usage_percent'] for s in self.cpu_usage_samples[-10:]])
            summary['avg_cpu_usage_percent'] = avg_cpu_usage

        return summary


class LocalModelClient:
    """Client for running self-hosted LLM models."""

    def __init__(self, config: LocalModelConfig):
        self.config = config
        self.model = None
        self.tokenizer = None
        self.device = None
        self.metrics = LocalModelMetrics()
        self._initialized = False
        self._model_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize the local model and tokenizer."""
        if self._initialized:
            return

        async with self._model_lock:
            if self._initialized:
                return

            logger.info(f"Initializing local model: {self.config.model_id}")

            try:
                # Determine device
                self.device = self._get_optimal_device()
                logger.info(f"Using device: {self.device}")

                # Load model based on type
                if self.config.model_type == 'fingpt':
                    await self._load_fingpt_model()
                elif self.config.model_type == 'llama':
                    await self._load_llama_model()
                else:
                    await self._load_generic_model()

                # Move model to device
                if self.device != 'cpu':
                    self.model = self.model.to(self.device)

                self._initialized = True
                logger.info("Local model initialized successfully")

            except Exception as e:
                logger.error(f"Failed to initialize local model: {e}")
                raise

    def _get_optimal_device(self) -> str:
        """Determine optimal device for model execution."""
        if self.config.device != "auto":
            return self.config.device

        if torch.cuda.is_available():
            # Find GPU with most free memory
            gpus = GPUtil.getGPUs()
            if gpus:
                best_gpu = max(gpus, key=lambda g: g.memoryFree)
                device = f"cuda:{best_gpu.id}"
                logger.info(f"Selected GPU {best_gpu.id} with {best_gpu.memoryFree}MB free memory")
                return device

        logger.warning("CUDA not available, using CPU")
        return "cpu"

    async def _load_fingpt_model(self):
        """Load FinGPT model with PEFT."""
        logger.info("Loading FinGPT model...")

        # FinGPT v3.2 configuration
        base_model = self.config.base_model or "NousResearch/Llama-2-7b-hf"
        peft_model = self.config.peft_model or "FinGPT/fingpt-sentiment_llama2-7b_lora"

        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            base_model,
            trust_remote_code=self.config.trust_remote_code
        )

        # Configure quantization
        quantization_config = None
        if self.config.enable_quantization and self.config.precision in ['int8', 'int4']:
            quantization_config = BitsAndBytesConfig(
                load_in_8bit=(self.config.precision == 'int8'),
                load_in_4bit=(self.config.precision == 'int4'),
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True
            )

        # Load base model
        self.model = AutoModelForCausalLM.from_pretrained(
            base_model,
            quantization_config=quantization_config,
            torch_dtype=torch.float16 if self.config.precision == 'fp16' else torch.float32,
            device_map="auto" if self.device == 'auto' else None,
            trust_remote_code=self.config.trust_remote_code
        )

        # Load PEFT adapter
        self.model = PeftModel.from_pretrained(self.model, peft_model)

        # Enable evaluation mode
        self.model.eval()

    async def _load_llama_model(self):
        """Load Llama 3.1 model."""
        logger.info("Loading Llama 3.1 model...")

        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.model_id,
            trust_remote_code=self.config.trust_remote_code
        )

        # Configure quantization
        quantization_config = None
        if self.config.enable_quantization and self.config.precision in ['int8', 'int4']:
            quantization_config = BitsAndBytesConfig(
                load_in_8bit=(self.config.precision == 'int8'),
                load_in_4bit=(self.config.precision == 'int4'),
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True
            )

        # Load model
        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.model_id,
            quantization_config=quantization_config,
            torch_dtype=torch.float16 if self.config.precision == 'fp16' else torch.float32,
            device_map="auto" if self.device == 'auto' else None,
            trust_remote_code=self.config.trust_remote_code
        )

        self.model.eval()

    async def _load_generic_model(self):
        """Load generic model."""
        logger.info(f"Loading generic model: {self.config.model_id}")

        # Load tokenizer and model
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.model_id,
            trust_remote_code=self.config.trust_remote_code
        )

        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.model_id,
            torch_dtype=torch.float16 if self.config.precision == 'fp16' else torch.float32,
            device_map="auto" if self.device == 'auto' else None,
            trust_remote_code=self.config.trust_remote_code
        )

        self.model.eval()

    async def generate_response(self, prompt: str, **kwargs) -> LLMResponse:
        """Generate response using local model."""
        if not self._initialized:
            await self.initialize()

        start_time = time.time()

        try:
            async with self._model_lock:
                # Tokenize input
                inputs = self.tokenizer(
                    prompt,
                    return_tensors="pt",
                    truncation=True,
                    max_length=self.config.max_length
                ).to(self.device)

                # Generate response
                with torch.no_grad():
                    outputs = self.model.generate(
                        **inputs,
                        max_new_tokens=kwargs.get('max_tokens', 512),
                        temperature=kwargs.get('temperature', self.config.temperature),
                        top_p=kwargs.get('top_p', self.config.top_p),
                        do_sample=True,
                        pad_token_id=self.tokenizer.eos_token_id
                    )

                # Decode response
                response_text = self.tokenizer.decode(
                    outputs[0][inputs.input_ids.shape[1]:],
                    skip_special_tokens=True
                )

                # Calculate metrics
                end_time = time.time()
                duration = end_time - start_time
                tokens_generated = len(outputs[0]) - inputs.input_ids.shape[1]

                # Record metrics
                self.metrics.record_inference(duration, tokens_generated)

                return LLMResponse(
                    content=response_text.strip(),
                    model=self.config.model_id,
                    provider="local",
                    tokens_used=len(outputs[0]),
                    cost_usd=0.0,  # Local model has no API costs
                    latency_ms=duration * 1000,
                    metadata={
                        'device': str(self.device),
                        'precision': self.config.precision,
                        'tokens_generated': tokens_generated,
                        'generation_speed_tokens_per_second': tokens_generated / duration if duration > 0 else 0
                    }
                )

        except Exception as e:
            self.metrics.record_error()
            logger.error(f"Local model inference failed: {e}")
            raise

    async def health_check(self) -> bool:
        """Check if the local model is healthy."""
        try:
            if not self._initialized:
                return False

            # Quick inference test
            test_prompt = "Test prompt for health check."
            response = await self.generate_response(test_prompt, max_tokens=10)

            return response is not None and len(response.content) > 0

        except Exception as e:
            logger.warning(f"Local model health check failed: {e}")
            return False

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get model performance metrics."""
        metrics = self.metrics.get_summary()

        # Add configuration info
        metrics.update({
            'model_id': self.config.model_id,
            'model_type': self.config.model_type,
            'device': str(self.device),
            'precision': self.config.precision,
            'quantization_enabled': self.config.enable_quantization,
            'max_length': self.config.max_length
        })

        return metrics

    async def close(self):
        """Clean up model resources."""
        if self.model is not None:
            del self.model
            self.model = None

        if self.tokenizer is not None:
            del self.tokenizer
            self.tokenizer = None

        # Clear GPU cache
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        self._initialized = False
        logger.info("Local model resources cleaned up")


class LocalModelOrchestrator:
    """Orchestrator for managing multiple local models."""

    def __init__(self):
        self.models: Dict[str, LocalModelClient] = {}
        self._default_model = None

    def add_model(self, name: str, config: LocalModelConfig, is_default: bool = False):
        """Add a local model to the orchestrator."""
        client = LocalModelClient(config)
        self.models[name] = client

        if is_default or self._default_model is None:
            self._default_model = name

        logger.info(f"Added local model '{name}' (default: {is_default})")

    async def initialize_all(self):
        """Initialize all models."""
        for name, client in self.models.items():
            try:
                await client.initialize()
                logger.info(f"Initialized model '{name}'")
            except Exception as e:
                logger.error(f"Failed to initialize model '{name}': {e}")

    async def generate_response(self, prompt: str, model_name: Optional[str] = None, **kwargs) -> LLMResponse:
        """Generate response using specified or default model."""
        model_name = model_name or self._default_model

        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")

        return await self.models[model_name].generate_response(prompt, **kwargs)

    async def health_check(self, model_name: Optional[str] = None) -> bool:
        """Check health of specified or all models."""
        if model_name:
            return await self.models[model_name].health_check()

        # Check all models
        results = {}
        for name, client in self.models.items():
            results[name] = await client.health_check()

        return all(results.values())

    def get_performance_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get performance metrics for all models."""
        metrics = {}
        for name, client in self.models.items():
            metrics[name] = client.get_performance_metrics()
        return metrics

    async def close_all(self):
        """Close all models."""
        for name, client in self.models.items():
            try:
                await client.close()
                logger.info(f"Closed model '{name}'")
            except Exception as e:
                logger.error(f"Error closing model '{name}': {e}")


# Factory functions for common model configurations

def create_fingpt_config(precision: str = "fp16", enable_quantization: bool = True) -> LocalModelConfig:
    """Create configuration for FinGPT model."""
    return LocalModelConfig(
        model_id="FinGPT/fingpt-sentiment_llama2-7b_lora",
        model_type="fingpt",
        base_model="NousResearch/Llama-2-7b-hf",
        peft_model="FinGPT/fingpt-sentiment_llama2-7b_lora",
        precision=precision,
        enable_quantization=enable_quantization,
        max_length=2048,
        temperature=0.1
    )


def create_llama_8b_config(precision: str = "fp16", enable_quantization: bool = True) -> LocalModelConfig:
    """Create configuration for Llama 3.1 8B model."""
    return LocalModelConfig(
        model_id="meta-llama/Meta-Llama-3.1-8B-Instruct",
        model_type="llama",
        precision=precision,
        enable_quantization=enable_quantization,
        max_length=8192,  # Llama 3.1 supports longer contexts
        temperature=0.1
    )


def create_llama_70b_config(precision: str = "fp16", enable_quantization: bool = True) -> LocalModelConfig:
    """Create configuration for Llama 3.1 70B model."""
    return LocalModelConfig(
        model_id="meta-llama/Meta-Llama-3.1-70B-Instruct",
        model_type="llama",
        precision=precision,
        enable_quantization=enable_quantization,
        max_length=8192,
        temperature=0.1,
        batch_size=1  # Large model requires smaller batch size
    )