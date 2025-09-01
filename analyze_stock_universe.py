#!/usr/bin/env python3
"""
Analyze FirstRate stock universe to understand scope of backfill
"""
import zipfile
import json
from pathlib import Path
from collections import defaultdict

def analyze_stock_universe():
    """Analyze available stock symbols and estimate processing requirements"""
    
    data_path = Path("/data/firstrate-data/stock")
    output_path = Path("/data/minute-bars/firstrate")
    
    print("🔍 Analyzing FirstRate Stock Universe...")
    print(f"Data Path: {data_path}")
    print(f"Output Path: {output_path}")
    
    # Discover symbols in ZIP files
    zip_analysis = {}
    all_symbols = set()
    
    for zip_file in data_path.glob("*.zip"):
        try:
            symbols_in_zip = set()
            file_count = 0
            
            with zipfile.ZipFile(zip_file, 'r') as zf:
                for file_name in zf.namelist():
                    if file_name.endswith('.txt'):
                        file_count += 1
                        if '_' in file_name:
                            symbol = file_name.split('_')[0]
                            if len(symbol) >= 1 and symbol.isalpha() and symbol.isupper():
                                symbols_in_zip.add(symbol)
                                all_symbols.add(symbol)
            
            zip_analysis[zip_file.name] = {
                "symbols": len(symbols_in_zip),
                "files": file_count,
                "size_mb": zip_file.stat().st_size / (1024 * 1024),
                "sample_symbols": sorted(list(symbols_in_zip))[:10]
            }
            
        except Exception as e:
            print(f"Error analyzing {zip_file}: {e}")
    
    # Analyze processed symbols
    processed_symbols = set()
    if output_path.exists():
        for symbol_dir in output_path.iterdir():
            if (symbol_dir.is_dir() and 
                symbol_dir.name.isupper() and 
                len(symbol_dir.name) <= 5 and
                symbol_dir.name.isalpha()):
                
                # Check if has actual data
                has_data = False
                data_years = []
                total_files = 0
                
                for year_dir in symbol_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        data_years.append(int(year_dir.name))
                        for month_dir in year_dir.iterdir():
                            if month_dir.is_dir():
                                parquet_files = list(month_dir.glob("*.parquet"))
                                if parquet_files:
                                    has_data = True
                                    total_files += len(parquet_files)
                
                if has_data:
                    processed_symbols.add(symbol_dir.name)
    
    # Calculate remaining work
    remaining_symbols = all_symbols - processed_symbols
    
    # Analysis by first letter
    letter_breakdown = defaultdict(int)
    for symbol in all_symbols:
        letter_breakdown[symbol[0]] += 1
    
    processed_letter_breakdown = defaultdict(int)
    for symbol in processed_symbols:
        processed_letter_breakdown[symbol[0]] += 1
    
    # Print comprehensive analysis
    print(f"\n{'='*60}")
    print("📊 FirstRate Stock Universe Analysis")
    print(f"{'='*60}")
    
    print(f"\n📦 ZIP File Analysis:")
    total_size = 0
    for zip_name, analysis in sorted(zip_analysis.items()):
        print(f"  {zip_name}:")
        print(f"    Symbols: {analysis['symbols']}")
        print(f"    Files: {analysis['files']}")
        print(f"    Size: {analysis['size_mb']:.1f} MB")
        print(f"    Sample: {', '.join(analysis['sample_symbols'][:5])}")
        total_size += analysis['size_mb']
    
    print(f"\nTotal ZIP size: {total_size:.1f} MB")
    
    print(f"\n📈 Symbol Statistics:")
    print(f"  Total symbols available: {len(all_symbols)}")
    print(f"  Already processed: {len(processed_symbols)}")
    print(f"  Remaining to process: {len(remaining_symbols)}")
    print(f"  Processing completion: {len(processed_symbols)/len(all_symbols)*100:.1f}%")
    
    print(f"\n🔤 Breakdown by First Letter:")
    for letter in sorted(letter_breakdown.keys()):
        total = letter_breakdown[letter]
        processed = processed_letter_breakdown.get(letter, 0)
        remaining = total - processed
        pct = processed/total*100 if total > 0 else 0
        print(f"  {letter}: {processed:3d}/{total:3d} ({pct:5.1f}%) - {remaining:3d} remaining")
    
    print(f"\n⏱️  Processing Estimates:")
    # Assume 2 minutes per symbol average (conservative)
    minutes_per_symbol = 2
    total_minutes = len(remaining_symbols) * minutes_per_symbol
    total_hours = total_minutes / 60
    
    print(f"  Remaining symbols: {len(remaining_symbols)}")
    print(f"  Est. time per symbol: {minutes_per_symbol} minutes")
    print(f"  Est. total processing time: {total_hours:.1f} hours ({total_minutes:.0f} minutes)")
    print(f"  Est. completion date: ~{total_hours/24:.1f} days if running 24/7")
    
    # Sample of remaining symbols
    if remaining_symbols:
        sample_remaining = sorted(list(remaining_symbols))[:20]
        print(f"\n🎯 Sample Remaining Symbols:")
        print(f"  {', '.join(sample_remaining)}")
        
        if len(remaining_symbols) > 20:
            print(f"  ... and {len(remaining_symbols) - 20} more")
    
    # Processing recommendation
    print(f"\n💡 Processing Recommendations:")
    if len(remaining_symbols) < 100:
        print(f"  ✅ Small backfill - can process all at once")
    elif len(remaining_symbols) < 1000:
        print(f"  ⚡ Medium backfill - process in batches of 50-100")
    else:
        print(f"  🏗️  Large backfill - process in parallel batches")
        print(f"  📊 Consider multi-process approach for {len(remaining_symbols)} symbols")
    
    # Save analysis to file
    analysis_data = {
        "analysis_timestamp": str(Path.cwd()),
        "zip_files": zip_analysis,
        "symbol_counts": {
            "total_available": len(all_symbols),
            "already_processed": len(processed_symbols),
            "remaining_to_process": len(remaining_symbols)
        },
        "letter_breakdown": dict(letter_breakdown),
        "processed_letter_breakdown": dict(processed_letter_breakdown),
        "remaining_symbols": sorted(list(remaining_symbols)),
        "processed_symbols": sorted(list(processed_symbols))
    }
    
    with open("firstrate_stock_universe_analysis.json", "w") as f:
        json.dump(analysis_data, f, indent=2)
    
    print(f"\n💾 Analysis saved to: firstrate_stock_universe_analysis.json")
    
    return {
        "total_symbols": len(all_symbols),
        "processed_symbols": len(processed_symbols), 
        "remaining_symbols": len(remaining_symbols),
        "remaining_list": sorted(list(remaining_symbols))
    }

if __name__ == "__main__":
    analyze_stock_universe()