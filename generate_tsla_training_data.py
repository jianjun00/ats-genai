#!/usr/bin/env python3
"""
Generate TSLA training data - wrapper script
"""
import asyncio
from generate_daily_training_data import generate_daily_training_data

if __name__ == "__main__":
    asyncio.run(generate_daily_training_data('TSLA'))