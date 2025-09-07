#!/usr/bin/env python3
"""
Financial LLM Demo with Open Source Models

This demo uses publicly available models for financial analysis:
- microsoft/DialoGPT-large for financial conversations
- Llama 2 7B (base model) for general financial analysis
- DistilBERT for financial sentiment analysis
"""

import asyncio
import time
import torch
from transformers import (
    AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig,
    AutoModelForSequenceClassification, pipeline
)
import psutil
import GPUtil
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def monitor_resources():
    """Monitor system resource usage."""
    print("\n💻 System Resource Usage:")

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

async def test_financial_sentiment():
    """Test financial sentiment analysis with DistilBERT."""
    print("💰 Testing DistilBERT Financial Sentiment Analysis")
    print("=" * 60)

    try:
        # Use a financial sentiment model
        model_name = "ProsusAI/finbert"

        print(f"📥 Loading FinBERT: {model_name}")

        # Create sentiment analysis pipeline
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model_name,
            device=0 if torch.cuda.is_available() else -1  # Use GPU if available
        )

        print("✅ FinBERT loaded successfully!")

        # Test financial news sentiment
        test_articles = [
            "Apple Inc. reported record-breaking quarterly earnings with revenue growing 15% year-over-year to $89.5 billion, significantly beating analyst expectations of $84.2 billion.",
            "Tesla faces mounting regulatory scrutiny as NHTSA investigates Autopilot system following three recent accidents. The probe could impact Tesla's Full Self-Driving rollout.",
            "Microsoft announces strategic $2.1 billion acquisition of leading AI startup, positioning the company for accelerated growth in artificial intelligence market.",
            "Federal Reserve signals potential interest rate cuts amid economic uncertainty, sparking volatility in financial markets as investors reassess risk portfolios.",
            "Amazon Web Services reports 28% revenue growth driven by enterprise cloud adoption and AI service expansion, exceeding Wall Street forecasts."
        ]

        print("\n🧪 Running Financial Sentiment Analysis Tests...")

        results = []
        total_time = 0

        for i, article in enumerate(test_articles, 1):
            print(f"\n--- Test {i}: {article[:60]}... ---")

            start_time = time.time()

            # Analyze sentiment
            result = sentiment_pipeline(article)

            end_time = time.time()
            analysis_time = end_time - start_time
            total_time += analysis_time

            print(f"   ⚡ Analysis Time: {analysis_time:.3f}s")
            print(f"   📊 Sentiment: {result[0]['label']} ({result[0]['score']:.3f} confidence)")

            results.append({
                "article": article[:80] + "...",
                "time_seconds": analysis_time,
                "sentiment": result[0]['label'],
                "confidence": result[0]['score']
            })

        print(f"\n🎯 FinBERT Performance Summary:")
        print(f"   📈 Total Tests: {len(test_articles)}")
        print(f"   ⚡ Average Time: {total_time/len(test_articles):.3f}s per analysis")
        print(f"   🚀 Throughput: {len(test_articles)/total_time:.1f} analyses per second")

        return True, results

    except Exception as e:
        print(f"❌ Financial sentiment test failed: {e}")
        return False, []

async def test_llama2_financial():
    """Test Llama 2 7B for general financial analysis."""
    print("\n🦙 Testing Llama 2 7B for Financial Analysis")
    print("=" * 60)

    try:
        model_id = "NousResearch/Llama-2-7b-hf"  # This should be publicly available

        print(f"📥 Loading Llama 2 7B: {model_id}")

        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
        tokenizer.pad_token = tokenizer.eos_token

        # Configure quantization for memory efficiency
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True
        )

        # Load model with quantization
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            quantization_config=quantization_config,
            device_map="auto",
            trust_remote_code=True,
            torch_dtype=torch.float16
        )
        model.eval()

        print("✅ Llama 2 7B loaded successfully!")

        # Test cases for financial analysis
        test_cases = [
            {
                "task": "Financial Entity Extraction",
                "prompt": """Extract financial entities from this news:

"Microsoft Corporation (NASDAQ: MSFT) reported Q3 earnings of $2.45 per share, beating estimates of $2.23. Revenue increased 18% to $52.9 billion driven by Azure cloud services growth of 31%."

Companies:
People:
Financial Metrics:
Key Events:"""
            },
            {
                "task": "Risk Assessment",
                "prompt": """Analyze the risk in this financial news:

"Tesla's Autopilot system under federal investigation following accidents. NHTSA reviewing data from 765,000 vehicles. Stock down 12% in pre-market trading."

Risk Level:
Impact:
Recommendation:"""
            }
        ]

        print("\n🧪 Running Financial Analysis Tests...")

        results = []
        total_time = 0

        for i, test_case in enumerate(test_cases, 1):
            print(f"\n--- Test {i}: {test_case['task']} ---")

            # Generate response
            inputs = tokenizer(test_case['prompt'], return_tensors="pt", truncation=True, max_length=512)
            inputs = {k: v.to(model.device) for k, v in inputs.items()}

            start_time = time.time()

            with torch.no_grad():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=150,
                    temperature=0.1,
                    do_sample=True,
                    pad_token_id=tokenizer.eos_token_id,
                    eos_token_id=tokenizer.eos_token_id
                )

            end_time = time.time()
            generation_time = end_time - start_time
            total_time += generation_time

            # Decode response
            response = tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)

            print(f"   ⚡ Generation Time: {generation_time:.2f}s")
            print(f"   📊 Analysis: {response.strip()[:200]}...")

            results.append({
                "task": test_case['task'],
                "time_seconds": generation_time,
                "analysis": response.strip()
            })

        print(f"\n🎯 Llama 2 Performance Summary:")
        print(f"   📈 Total Tests: {len(test_cases)}")
        print(f"   ⚡ Average Time: {total_time/len(test_cases):.2f}s per analysis")
        print(f"   🚀 Throughput: {len(test_cases)/total_time:.2f} analyses per second")

        return True, results

    except Exception as e:
        print(f"❌ Llama 2 test failed: {e}")
        logger.exception("Detailed error:")
        return False, []

async def test_lightweight_text_generation():
    """Test with a lightweight text generation model."""
    print("\n🤖 Testing GPT-2 for Financial Text Generation")
    print("=" * 60)

    try:
        model_name = "gpt2"

        print(f"📥 Loading GPT-2: {model_name}")

        # Load tokenizer and model
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        tokenizer.pad_token = tokenizer.eos_token

        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            device_map="auto" if torch.cuda.is_available() else None
        )
        model.eval()

        print("✅ GPT-2 loaded successfully!")

        # Test financial text generation
        prompts = [
            "Apple's quarterly earnings report shows",
            "The Federal Reserve's decision to raise interest rates will",
            "Tesla's stock price volatility is driven by"
        ]

        print("\n🧪 Running Financial Text Generation Tests...")

        results = []
        total_time = 0

        for i, prompt in enumerate(prompts, 1):
            print(f"\n--- Test {i}: {prompt} ---")

            # Generate text
            inputs = tokenizer(prompt, return_tensors="pt")
            inputs = {k: v.to(model.device) for k, v in inputs.items()}

            start_time = time.time()

            with torch.no_grad():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=100,
                    temperature=0.7,
                    do_sample=True,
                    pad_token_id=tokenizer.eos_token_id
                )

            end_time = time.time()
            generation_time = end_time - start_time
            total_time += generation_time

            # Decode response
            generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
            new_text = generated_text[len(prompt):].strip()

            print(f"   ⚡ Generation Time: {generation_time:.3f}s")
            print(f"   📊 Generated: {new_text[:150]}...")

            results.append({
                "prompt": prompt,
                "time_seconds": generation_time,
                "generated_text": new_text
            })

        print(f"\n🎯 GPT-2 Performance Summary:")
        print(f"   📈 Total Tests: {len(prompts)}")
        print(f"   ⚡ Average Time: {total_time/len(prompts):.3f}s per generation")
        print(f"   🚀 Throughput: {len(prompts)/total_time:.1f} generations per second")

        return True, results

    except Exception as e:
        print(f"❌ GPT-2 test failed: {e}")
        logger.exception("Detailed error:")
        return False, []

async def main():
    """Main demo function."""
    print("🚀 Open Source Financial LLM Performance Demo")
    print("=" * 60)

    # Check initial resources
    monitor_resources()

    # Test 1: Financial Sentiment Analysis with FinBERT
    print("\n" + "="*60)
    sentiment_success, sentiment_results = await test_financial_sentiment()

    if sentiment_success:
        print("\n💻 Resources after sentiment analysis:")
        monitor_resources()

    # Clear GPU cache
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    # Test 2: Financial Analysis with GPT-2 (lightweight alternative)
    print("\n" + "="*60)
    gpt2_success, gpt2_results = await test_lightweight_text_generation()

    if gpt2_success:
        print("\n💻 Resources after text generation:")
        monitor_resources()

    # Clear GPU cache
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    # Test 3: Try Llama 2 (if resources allow)
    print("\n" + "="*60)
    llama_success, llama_results = await test_llama2_financial()

    if llama_success:
        print("\n💻 Resources after Llama 2:")
        monitor_resources()

    # Final summary
    print("\n" + "="*60)
    print("🎯 DEMO SUMMARY")
    print("="*60)

    print(f"FinBERT Financial Sentiment: {'✅ Success' if sentiment_success else '❌ Failed'}")
    if sentiment_success and sentiment_results:
        avg_time = sum(r['time_seconds'] for r in sentiment_results) / len(sentiment_results)
        print(f"   - Average inference time: {avg_time:.3f}s")
        print(f"   - Specialized for financial sentiment analysis")

    print(f"GPT-2 Financial Text Generation: {'✅ Success' if gpt2_success else '❌ Failed'}")
    if gpt2_success and gpt2_results:
        avg_time = sum(r['time_seconds'] for r in gpt2_results) / len(gpt2_results)
        print(f"   - Average generation time: {avg_time:.3f}s")
        print(f"   - Lightweight model for basic text generation")

    print(f"Llama 2 Financial Analysis: {'✅ Success' if llama_success else '❌ Failed'}")
    if llama_success and llama_results:
        avg_time = sum(r['time_seconds'] for r in llama_results) / len(llama_results)
        print(f"   - Average analysis time: {avg_time:.2f}s")
        print(f"   - Advanced model for comprehensive analysis")

    success_count = sum([sentiment_success, gpt2_success, llama_success])

    if success_count >= 2:
        print(f"\n🎉 Demo successful! {success_count}/3 models working.")
        print("\n💡 Performance comparison:")
        print("   - FinBERT sentiment: Ultra-fast (< 0.1s), specialized")
        print("   - GPT-2 generation: Fast (< 1s), versatile")
        print("   - Llama 2 analysis: Slower (2-5s), comprehensive")

        print("\n🚀 Next steps for production:")
        print("   1. Set up model serving with FastAPI")
        print("   2. Implement model selection based on task type")
        print("   3. Add caching and batch processing")
        print("   4. Deploy with Docker containers")
    else:
        print(f"\n⚠️  Limited success: {success_count}/3 models working")
        print("   Consider using cloud APIs for more reliable access")

    # Save results
    demo_results = {
        "timestamp": time.time(),
        "sentiment_success": sentiment_success,
        "sentiment_results": sentiment_results,
        "gpt2_success": gpt2_success,
        "gpt2_results": gpt2_results,
        "llama_success": llama_success,
        "llama_results": llama_results,
        "gpu_available": torch.cuda.is_available(),
        "gpu_count": torch.cuda.device_count() if torch.cuda.is_available() else 0
    }

    with open("alternative_demo_results.json", "w") as f:
        json.dump(demo_results, f, indent=2, default=str)

    print(f"\n💾 Results saved to alternative_demo_results.json")

if __name__ == "__main__":
    asyncio.run(main())