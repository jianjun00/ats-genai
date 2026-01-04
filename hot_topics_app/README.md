# Hot Topics Analysis Application

## Overview
Unified application that:
1. **Gets real data** from Reddit WallStreetBets
2. **Uses Grok as main reasoning engine** for market analysis
3. **Uses OpenAI for cross-validation** (optional)
4. **No fake rules** - pure AI reasoning for impact analysis

## Architecture
```
Real Data (Reddit) → Grok Analysis → OpenAI Validation → Consensus Scoring
```

## Key Features
- ✅ **Real social media data** from Reddit WSB API
- ✅ **Grok-3 as primary analyst** for market impact reasoning
- ✅ **GPT-4 cross-validation** for consensus scoring
- ✅ **Pure AI reasoning** - no manual impact rules
- ✅ **Unified application** in single directory

## Usage
```bash
cd /home/jianjun/ats-genai-data/hot_topics_app
python3 main.py
```

## Output
- Console report with AI analysis
- JSON results saved to `results.json`
- Consensus scores between AI models
- Market impact predictions with confidence

## No Fake Logic
- Data comes from real Reddit API
- Impact analysis done by Grok reasoning
- Cross-validation by OpenAI reasoning  
- No hardcoded rules or synthetic data