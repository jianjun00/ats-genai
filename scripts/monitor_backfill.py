#!/usr/bin/env python3
"""
Monitor FirstRate backfill progress by counting files
"""

from pathlib import Path
from datetime import datetime

def main():
    print(f"🔍 FirstRate Backfill Monitor - {datetime.now().strftime('%H:%M:%S')}")
    
    base_path = Path("/data/minute-bars/firstrate")
    
    # Count complete files by letter
    letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 
              'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    
    total_complete = 0
    total_sample = 0
    
    print("📊 Progress by Letter:")
    
    for letter in letters:
        letter_path = base_path / letter
        if letter_path.exists():
            # Count different file types quickly
            complete_count = len(list(letter_path.rglob("*_complete.parquet")))
            sample_count = len(list(letter_path.rglob("*_sample.parquet")))
            
            total_complete += complete_count
            total_sample += sample_count
            
            if complete_count > 0 or sample_count > 0:
                status = "🔄" if complete_count < sample_count else "✅" if complete_count > 0 else "⚪"
                print(f"   {status} {letter}: {complete_count} complete, {sample_count} sample")
        else:
            print(f"   ❌ {letter}: Directory missing")
    
    print(f"\n📈 Summary:")
    print(f"   Total complete files: {total_complete:,}")
    print(f"   Total sample files: {total_sample:,}")
    print(f"   Grand total: {total_complete + total_sample:,} parquet files")

if __name__ == "__main__":
    main()