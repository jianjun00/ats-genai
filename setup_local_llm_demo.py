#!/usr/bin/env python3
"""
Local LLM Demo Setup Script

This script sets up FinGPT v3.2 + Llama 3.1 8B for performance testing and demo.
It handles all the dependencies, model downloads, and initial configuration.
"""

import os
import sys
import subprocess
import time
import json
from pathlib import Path

def run_command(cmd, description="", check=True):
    """Run a command with proper error handling."""
    print(f"🔄 {description}...")
    print(f"   Running: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=check, 
                               capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - Success")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} - Failed")
            print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ {description} - Exception: {e}")
        return False

def check_gpu():
    """Check GPU availability and specifications."""
    print("🔍 Checking GPU availability...")
    
    gpu_ok = run_command("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits", 
                        "GPU Detection", check=False)
    
    if not gpu_ok:
        print("⚠️  No NVIDIA GPU detected. Models will run on CPU (much slower)")
        return False
    
    # Get GPU memory
    result = subprocess.run("nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits", 
                           shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        free_memory = int(result.stdout.strip())
        print(f"🎯 GPU Memory Available: {free_memory}MB")
        
        if free_memory < 16000:
            print("⚠️  Warning: Less than 16GB GPU memory available. May need quantization.")
        elif free_memory >= 20000:
            print("✅ Excellent! 20GB+ GPU memory available - perfect for FinGPT + Llama 8B")
        
        return True
    
    return False

def install_dependencies():
    """Install required Python dependencies."""
    print("📦 Installing dependencies...")
    
    # Core ML dependencies
    deps = [
        "torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121",
        "transformers>=4.36.0",
        "accelerate>=0.25.0", 
        "peft>=0.7.0",
        "bitsandbytes>=0.41.0",
        "sentencepiece",
        "protobuf",
        "numpy",
        "psutil",
        "GPUtil",
        "fastapi",
        "uvicorn[standard]",
        "aiohttp",
        "requests"
    ]
    
    for dep in deps:
        success = run_command(f"pip install {dep}", f"Installing {dep.split()[0]}")
        if not success and dep.split()[0] in ["torch", "transformers"]:
            print(f"❌ Critical dependency failed: {dep}")
            return False
    
    return True

def setup_model_directory():
    """Create directory for model cache."""
    models_dir = Path.home() / ".cache" / "huggingface"
    models_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Model cache directory: {models_dir}")
    return str(models_dir)

def create_demo_script():
    """Create demo script for testing models."""
    demo_script = """#!/usr/bin/env python3
import asyncio
import time
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel
import psutil
import GPUtil

async def test_fingpt():
    print("🤖 Testing FinGPT v3.2 for Financial Sentiment Analysis")
    print("=" * 60)
    
    try:
        # Configuration for FinGPT
        base_model = "NousResearch/Llama-2-7b-hf"
        peft_model = "FinGPT/fingpt-sentiment_llama2-7b_lora" 
        
        print(f"📥 Loading base model: {base_model}")
        
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(base_model, trust_remote_code=True)
        tokenizer.pad_token = tokenizer.eos_token
        
        # Configure quantization for memory efficiency
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True
        )
        
        # Load base model with quantization
        model = AutoModelForCausalLM.from_pretrained(
            base_model,
            quantization_config=quantization_config,
            device_map="auto",
            trust_remote_code=True
        )
        
        print(f"📥 Loading FinGPT LoRA adapter: {peft_model}")
        # Load PEFT adapter
        model = PeftModel.from_pretrained(model, peft_model)
        model.eval()
        
        print("✅ FinGPT v3.2 loaded successfully!")
        
        # Test financial sentiment analysis
        test_articles = [
            "Apple Inc. reported record-breaking quarterly earnings with revenue growing 15% year-over-year, significantly beating analyst expectations.",
            "Tesla faces regulatory scrutiny as NHTSA investigates Autopilot system following recent accidents, raising safety concerns.",
            "Microsoft announces strategic partnership with leading AI company, positioning for growth in artificial intelligence market.",
        ]
        
        print("\\n🧪 Running Financial Sentiment Analysis Tests...")
        
        for i, article in enumerate(test_articles, 1):
            print(f"\\nTest {i}: {article[:60]}...")
            
            # Create financial sentiment prompt
            prompt = f'''Analyze the sentiment of this financial news for trading decisions:
            
News: {article}

Provide your analysis in this format:
Sentiment: [positive/negative/neutral]
Confidence: [0-100]%
Signal: [buy/hold/sell]
Reasoning: [brief explanation]

Analysis:'''

            # Tokenize and generate
            inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024)
            inputs = {k: v.to(model.device) for k, v in inputs.items()}
            
            start_time = time.time()
            
            with torch.no_grad():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=200,
                    temperature=0.1,
                    do_sample=True,
                    pad_token_id=tokenizer.eos_token_id
                )
            
            end_time = time.time()
            
            # Decode response
            response = tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)
            
            print(f"   ⚡ Generation Time: {end_time - start_time:.2f}s")
            print(f"   📊 Result: {response.strip()}")
        
        return True
        
    except Exception as e:
        print(f"❌ FinGPT test failed: {e}")
        return False

async def test_llama_8b():
    print("\\n🦙 Testing Llama 3.1 8B for General Financial Analysis") 
    print("=" * 60)
    
    try:
        model_id = "meta-llama/Meta-Llama-3.1-8B-Instruct"
        
        print(f"📥 Loading Llama 3.1 8B: {model_id}")
        
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        tokenizer.pad_token = tokenizer.eos_token
        
        # Configure quantization
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True
        )
        
        # Load model
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            quantization_config=quantization_config,
            device_map="auto",
            trust_remote_code=True
        )
        model.eval()
        
        print("✅ Llama 3.1 8B loaded successfully!")
        
        # Test entity recognition and event detection
        test_prompt = '''<|begin_of_text|><|start_header_id|>system<|end_header_id|>
You are a financial analyst AI. Extract entities and events from financial news.<|eot_id|>

<|start_header_id|>user<|end_header_id|>
Analyze this financial news and extract:
1. Companies mentioned (with tickers if known)
2. Key events or announcements
3. Financial metrics mentioned
4. Market impact assessment

News: "Microsoft Corporation (NASDAQ: MSFT) announced today a strategic acquisition of AI startup for $2.1 billion, expecting to boost cloud revenue by 25% over the next fiscal year. The deal is expected to close in Q2 2025."

<|start_header_id|>assistant<|end_header_id|>'''

        # Generate response
        inputs = tokenizer(test_prompt, return_tensors="pt", truncation=True, max_length=2048)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}
        
        start_time = time.time()
        
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=300,
                temperature=0.1,
                do_sample=True,
                pad_token_id=tokenizer.eos_token_id
            )
        
        end_time = time.time()
        
        # Decode response
        response = tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)
        
        print(f"\\n🧪 Entity & Event Extraction Test:")
        print(f"   ⚡ Generation Time: {end_time - start_time:.2f}s")
        print(f"   📊 Analysis: {response.strip()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Llama 3.1 8B test failed: {e}")
        return False

def monitor_resources():
    print("\\n💻 System Resource Usage:")
    
    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"   🖥️  CPU Usage: {cpu_percent}%")
    
    # Memory usage
    memory = psutil.virtual_memory()
    print(f"   🧠 RAM Usage: {memory.used // (1024**3):.1f}GB / {memory.total // (1024**3):.1f}GB ({memory.percent}%)")
    
    # GPU usage
    try:
        gpus = GPUtil.getGPUs()
        if gpus:
            gpu = gpus[0]
            print(f"   🎮 GPU Usage: {gpu.load * 100:.1f}%")
            print(f"   📱 GPU Memory: {gpu.memoryUsed:.0f}MB / {gpu.memoryTotal:.0f}MB ({gpu.memoryUsed/gpu.memoryTotal*100:.1f}%)")
    except:
        print("   🎮 GPU monitoring not available")

async def main():
    print("🚀 FinGPT v3.2 + Llama 3.1 8B Demo")
    print("=" * 60)
    
    # Check initial resources
    monitor_resources()
    
    # Test FinGPT for financial sentiment
    fingpt_success = await test_fingpt()
    
    if fingpt_success:
        monitor_resources()
    
    # Test Llama for general financial analysis  
    llama_success = await test_llama_8b()
    
    if llama_success:
        monitor_resources()
    
    print("\\n🎯 Demo Summary:")
    print(f"   FinGPT v3.2 Financial Sentiment: {'✅ Success' if fingpt_success else '❌ Failed'}")
    print(f"   Llama 3.1 8B General Analysis: {'✅ Success' if llama_success else '❌ Failed'}")
    
    if fingpt_success and llama_success:
        print("\\n🎉 Demo completed successfully! Both models are working.")
        print("\\n💡 Next steps:")
        print("   - Integrate with news processing pipeline")
        print("   - Set up model serving with FastAPI")
        print("   - Deploy production system with Docker")
    else:
        print("\\n⚠️  Some models failed. Check error messages above.")

if __name__ == "__main__":
    asyncio.run(main())
"""
    
    with open("demo_local_llm.py", "w") as f:
        f.write(demo_script)
    
    print("📝 Created demo_local_llm.py")
    return True

def main():
    """Main setup function."""
    print("🚀 FinGPT v3.2 + Llama 3.1 8B Demo Setup")
    print("=" * 60)
    
    # Check system requirements
    print("🔍 System Requirements Check:")
    gpu_available = check_gpu()
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Failed to install dependencies. Exiting.")
        sys.exit(1)
    
    # Setup model directory
    model_dir = setup_model_directory()
    
    # Create demo script
    if not create_demo_script():
        print("❌ Failed to create demo script. Exiting.")
        sys.exit(1)
    
    print("\n✅ Setup completed successfully!")
    print("\n🎯 Next Steps:")
    print("1. Run the demo: python3 demo_local_llm.py")
    print("2. Monitor GPU usage: watch -n 1 nvidia-smi")
    print("3. Check performance metrics in the output")
    
    if gpu_available:
        print("\n💡 Your RTX 4090 with 24GB VRAM is perfect for this demo!")
        print("   Expected performance:")
        print("   - FinGPT sentiment analysis: ~2-3 seconds")
        print("   - Llama 8B analysis: ~3-4 seconds")
        print("   - Total GPU memory usage: ~14-16GB")
    else:
        print("\n⚠️  Running on CPU will be significantly slower (30-60 seconds per inference)")

if __name__ == "__main__":
    main()