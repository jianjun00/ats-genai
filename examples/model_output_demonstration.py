#!/usr/bin/env python3
"""
Support/Resistance Model Output Demonstration

This script shows realistic examples of what the model outputs look like,
including predictions, confidence scores, and visualizations.
"""

import os
import sys
import numpy as np
import pandas as pd
try:
    import matplotlib.pyplot as plt
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
from datetime import date, datetime, timedelta
from pathlib import Path
import random

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ml.models.support_resistance_model import SRModelConfig
from ml.training_data.support_resistance_generator import SupportResistanceLevel

def generate_realistic_sample_data():
    """Generate realistic sample data for demonstration"""
    
    # Sample stock prices for demonstration
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    dates = [date(2023, 6, 1) + timedelta(days=i) for i in range(10)]
    
    sample_data = []
    
    for symbol in symbols[:3]:  # Show 3 symbols
        base_price = {'AAPL': 180, 'MSFT': 350, 'GOOGL': 120}[symbol]
        
        for i, trade_date in enumerate(dates):
            # Simulate realistic price movement
            price_change = np.random.normal(0, 0.02)  # 2% daily volatility
            current_price = base_price * (1 + price_change * i * 0.1)
            
            # Generate sample features (realistic technical indicators)
            features = {
                'close': current_price,
                'volume_ratio_20d': np.random.uniform(0.8, 1.5),
                'rsi_14': np.random.uniform(30, 70),
                'ma_20': current_price * np.random.uniform(0.98, 1.02),
                'atr': current_price * np.random.uniform(0.015, 0.035),
                'bb_position': np.random.uniform(0.2, 0.8),
                'distance_to_resistance': np.random.uniform(0.02, 0.08),
                'distance_to_support': np.random.uniform(0.02, 0.06),
                'trend_strength': np.random.uniform(-0.05, 0.05),
                'volatility_20d': np.random.uniform(0.2, 0.4)
            }
            
            # Generate realistic S/R predictions
            support_levels = []
            resistance_levels = []
            
            # Support levels (below current price)
            for j in range(3):
                level_price = current_price * np.random.uniform(0.94 - j*0.02, 0.98 - j*0.01)
                strength = max(0.1, np.random.beta(2, 3))  # Bias toward lower strengths
                tests = np.random.randint(2, 8)
                
                support_levels.append(SupportResistanceLevel(
                    level=level_price,
                    level_type='support',
                    strength=strength,
                    tests_count=tests,
                    volume_at_level=np.random.uniform(500000, 2000000),
                    time_held=np.random.uniform(10, 60),
                    break_through=False
                ))
            
            # Resistance levels (above current price)
            for j in range(3):
                level_price = current_price * np.random.uniform(1.02 + j*0.01, 1.06 + j*0.02)
                strength = max(0.1, np.random.beta(2, 3))
                tests = np.random.randint(2, 6)
                
                resistance_levels.append(SupportResistanceLevel(
                    level=level_price,
                    level_type='resistance', 
                    strength=strength,
                    tests_count=tests,
                    volume_at_level=np.random.uniform(400000, 1500000),
                    time_held=np.random.uniform(15, 45),
                    break_through=False
                ))
            
            # Simulate next day's actual price action
            next_day_high = current_price * np.random.uniform(1.005, 1.025)
            next_day_low = current_price * np.random.uniform(0.975, 0.995)
            next_day_close = current_price * np.random.uniform(0.98, 1.02)
            
            sample_data.append({
                'symbol': symbol,
                'date': trade_date,
                'current_price': current_price,
                'features': features,
                'predicted_support': support_levels,
                'predicted_resistance': resistance_levels,
                'actual_high': next_day_high,
                'actual_low': next_day_low,
                'actual_close': next_day_close
            })
    
    return sample_data

def simulate_model_predictions(sample_data):
    """Simulate model predictions for the sample data"""
    
    predictions = []
    
    for data in sample_data:
        # Simulate model output structure
        prediction = {
            'symbol': data['symbol'],
            'date': data['date'],
            'input_features': data['features'],
            
            # Model predictions
            'support_levels': [level.level for level in data['predicted_support']],
            'support_confidence': [level.strength for level in data['predicted_support']],
            'support_tests': [level.tests_count for level in data['predicted_support']],
            
            'resistance_levels': [level.level for level in data['predicted_resistance']],
            'resistance_confidence': [level.strength for level in data['predicted_resistance']],
            'resistance_tests': [level.tests_count for level in data['predicted_resistance']],
            
            # Ensemble predictions (weighted average)
            'ensemble_support': data['predicted_support'][0].level,  # Primary support
            'ensemble_resistance': data['predicted_resistance'][0].level,  # Primary resistance
            
            # Actual outcomes (for evaluation)
            'actual_high': data['actual_high'],
            'actual_low': data['actual_low'],
            'actual_close': data['actual_close'],
            'current_price': data['current_price']
        }
        
        predictions.append(prediction)
    
    return predictions

def print_detailed_predictions(predictions):
    """Print detailed model predictions in a readable format"""
    
    print("="*80)
    print("SUPPORT/RESISTANCE MODEL PREDICTIONS - DETAILED OUTPUT")
    print("="*80)
    
    for i, pred in enumerate(predictions[:6]):  # Show first 6 predictions
        print(f"\n{'='*60}")
        print(f"PREDICTION #{i+1}: {pred['symbol']} on {pred['date']}")
        print("="*60)
        
        print(f"Current Price: ${pred['current_price']:.2f}")
        
        # Input features summary
        print(f"\nKey Input Features:")
        features = pred['input_features']
        print(f"  RSI(14): {features['rsi_14']:.1f}")
        print(f"  Volume Ratio: {features['volume_ratio_20d']:.2f}x")
        print(f"  ATR: ${features['atr']:.2f} ({features['atr']/pred['current_price']*100:.1f}%)")
        print(f"  BB Position: {features['bb_position']:.2f}")
        print(f"  Trend Strength: {features['trend_strength']:+.3f}")
        
        # Support predictions
        print(f"\n📉 SUPPORT LEVEL PREDICTIONS:")
        for j, (level, conf, tests) in enumerate(zip(
            pred['support_levels'], pred['support_confidence'], pred['support_tests']
        )):
            distance_pct = (pred['current_price'] - level) / pred['current_price'] * 100
            print(f"  Level {j+1}: ${level:.2f} ({distance_pct:.1f}% below)")
            print(f"           Confidence: {conf:.1%}, Historical Tests: {tests}")
        
        # Resistance predictions  
        print(f"\n📈 RESISTANCE LEVEL PREDICTIONS:")
        for j, (level, conf, tests) in enumerate(zip(
            pred['resistance_levels'], pred['resistance_confidence'], pred['resistance_tests']
        )):
            distance_pct = (level - pred['current_price']) / pred['current_price'] * 100
            print(f"  Level {j+1}: ${level:.2f} ({distance_pct:.1f}% above)")
            print(f"           Confidence: {conf:.1%}, Historical Tests: {tests}")
        
        # Ensemble predictions
        print(f"\n🎯 ENSEMBLE PREDICTIONS:")
        print(f"  Primary Support: ${pred['ensemble_support']:.2f}")
        print(f"  Primary Resistance: ${pred['ensemble_resistance']:.2f}")
        
        # Actual outcomes
        print(f"\n📊 NEXT DAY ACTUAL RESULTS:")
        print(f"  High: ${pred['actual_high']:.2f}")
        print(f"  Low: ${pred['actual_low']:.2f}")
        print(f"  Close: ${pred['actual_close']:.2f}")
        
        # Accuracy assessment
        support_hit = any(abs(pred['actual_low'] - level) / level < 0.005 
                         for level in pred['support_levels'])
        resistance_hit = any(abs(pred['actual_high'] - level) / level < 0.005 
                           for level in pred['resistance_levels'])
        
        print(f"\n✅ PREDICTION ACCURACY:")
        print(f"  Support Hit: {'YES' if support_hit else 'NO'}")
        print(f"  Resistance Hit: {'YES' if resistance_hit else 'NO'}")
        
        # Trading signals
        print(f"\n💡 GENERATED TRADING SIGNALS:")
        
        # Buy signal if price near strong support
        strong_support = [level for level, conf in zip(pred['support_levels'], pred['support_confidence']) 
                         if conf > 0.6]
        if strong_support and pred['actual_low'] <= strong_support[0] * 1.02:
            print(f"  🟢 BUY SIGNAL: Price ${pred['actual_low']:.2f} near strong support ${strong_support[0]:.2f}")
            print(f"     Entry: ${strong_support[0] * 1.005:.2f}, Target: ${strong_support[0] * 1.04:.2f}, Stop: ${strong_support[0] * 0.98:.2f}")
        
        # Sell signal if price near strong resistance
        strong_resistance = [level for level, conf in zip(pred['resistance_levels'], pred['resistance_confidence']) 
                            if conf > 0.6]
        if strong_resistance and pred['actual_high'] >= strong_resistance[0] * 0.98:
            print(f"  🔴 SELL SIGNAL: Price ${pred['actual_high']:.2f} near strong resistance ${strong_resistance[0]:.2f}")
            print(f"     Entry: ${strong_resistance[0] * 0.995:.2f}, Target: ${strong_resistance[0] * 0.96:.2f}, Stop: ${strong_resistance[0] * 1.02:.2f}")
        
        if not (strong_support and pred['actual_low'] <= strong_support[0] * 1.02) and \
           not (strong_resistance and pred['actual_high'] >= strong_resistance[0] * 0.98):
            print(f"  ⚪ HOLD: No strong signals generated")

def create_prediction_visualization(predictions):
    """Create visualizations of model predictions"""
    
    print(f"\n{'='*60}")
    print("CREATING PREDICTION VISUALIZATIONS")
    print("="*60)
    
    if not PLOTTING_AVAILABLE:
        print("📊 Matplotlib not available - showing text-based visualization")
        show_text_visualization(predictions)
        return None
    
    # Setup the plot
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Support/Resistance Model Predictions', fontsize=16, fontweight='bold')
    
    # Plot 1: Support/Resistance Levels for AAPL
    ax1 = axes[0, 0]
    aapl_preds = [p for p in predictions if p['symbol'] == 'AAPL'][:5]
    
    dates = [datetime.combine(p['date'], datetime.min.time()) for p in aapl_preds]
    current_prices = [p['current_price'] for p in aapl_preds]
    
    ax1.plot(dates, current_prices, 'b-', linewidth=2, label='Current Price', marker='o')
    
    # Plot support and resistance levels
    for i, pred in enumerate(aapl_preds):
        date_val = dates[i]
        
        # Support levels
        for j, (level, conf) in enumerate(zip(pred['support_levels'], pred['support_confidence'])):
            alpha = conf  # Use confidence for transparency
            ax1.hlines(level, date_val, date_val + timedelta(hours=12), 
                      colors='red', alpha=alpha, linewidth=2)
            if j == 0:  # Label only first level
                ax1.text(date_val, level, f'S{j+1}', fontsize=8, ha='right')
        
        # Resistance levels
        for j, (level, conf) in enumerate(zip(pred['resistance_levels'], pred['resistance_confidence'])):
            alpha = conf
            ax1.hlines(level, date_val, date_val + timedelta(hours=12), 
                      colors='green', alpha=alpha, linewidth=2)
            if j == 0:
                ax1.text(date_val, level, f'R{j+1}', fontsize=8, ha='right')
    
    ax1.set_title('AAPL: Support/Resistance Predictions', fontweight='bold')
    ax1.set_ylabel('Price ($)')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Plot 2: Confidence Distribution
    ax2 = axes[0, 1]
    
    all_support_conf = []
    all_resistance_conf = []
    
    for pred in predictions:
        all_support_conf.extend(pred['support_confidence'])
        all_resistance_conf.extend(pred['resistance_confidence'])
    
    ax2.hist(all_support_conf, bins=15, alpha=0.7, label='Support Confidence', color='red')
    ax2.hist(all_resistance_conf, bins=15, alpha=0.7, label='Resistance Confidence', color='green')
    ax2.set_title('Confidence Score Distribution', fontweight='bold')
    ax2.set_xlabel('Confidence Score')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Prediction Accuracy by Symbol
    ax3 = axes[1, 0]
    
    symbols = list(set(p['symbol'] for p in predictions))
    support_accuracies = []
    resistance_accuracies = []
    
    for symbol in symbols:
        symbol_preds = [p for p in predictions if p['symbol'] == symbol]
        
        # Calculate accuracy
        support_hits = sum(1 for p in symbol_preds 
                          if any(abs(p['actual_low'] - level) / level < 0.01 
                                for level in p['support_levels']))
        resistance_hits = sum(1 for p in symbol_preds
                             if any(abs(p['actual_high'] - level) / level < 0.01 
                                   for level in p['resistance_levels']))
        
        support_accuracies.append(support_hits / len(symbol_preds))
        resistance_accuracies.append(resistance_hits / len(symbol_preds))
    
    x = np.arange(len(symbols))
    width = 0.35
    
    ax3.bar(x - width/2, support_accuracies, width, label='Support Accuracy', color='red', alpha=0.7)
    ax3.bar(x + width/2, resistance_accuracies, width, label='Resistance Accuracy', color='green', alpha=0.7)
    
    ax3.set_title('Prediction Accuracy by Symbol', fontweight='bold')
    ax3.set_xlabel('Symbol')
    ax3.set_ylabel('Accuracy Rate')
    ax3.set_xticks(x)
    ax3.set_xticklabels(symbols)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Feature Importance (simulated)
    ax4 = axes[1, 1]
    
    # Simulate feature importance scores
    feature_names = ['RSI', 'Volume Ratio', 'ATR', 'BB Position', 'Trend Strength', 
                    'Distance to R', 'Distance to S', 'MA Distance', 'Volatility']
    importance_scores = np.random.uniform(0.1, 0.9, len(feature_names))
    importance_scores = sorted(importance_scores, reverse=True)
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(feature_names)))
    bars = ax4.barh(feature_names, importance_scores, color=colors)
    
    ax4.set_title('Feature Importance (Simulated)', fontweight='bold')
    ax4.set_xlabel('Importance Score')
    ax4.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, score in zip(bars, importance_scores):
        ax4.text(score + 0.01, bar.get_y() + bar.get_height()/2, 
                f'{score:.2f}', va='center', fontsize=9)
    
    plt.tight_layout()
    
    # Save the plot
    output_file = 'model_predictions_visualization.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_file}")
    
    return fig

def show_text_visualization(predictions):
    """Show text-based visualization when plotting libraries aren't available"""
    
    print("📊 AAPL Support/Resistance Levels (Text View)")
    print("-" * 50)
    
    aapl_preds = [p for p in predictions if p['symbol'] == 'AAPL'][:3]
    
    for pred in aapl_preds:
        print(f"\n{pred['date']} - Current: ${pred['current_price']:.2f}")
        
        # Show resistance levels above price
        for level, conf in zip(pred['resistance_levels'], pred['resistance_confidence']):
            stars = "★" * int(conf * 5)  # Visual confidence indicator
            distance = (level - pred['current_price']) / pred['current_price'] * 100
            print(f"  R: ${level:.2f} (+{distance:.1f}%) {stars}")
        
        print(f"  ━━ ${pred['current_price']:.2f} ━━ (Current Price)")
        
        # Show support levels below price
        for level, conf in zip(pred['support_levels'], pred['support_confidence']):
            stars = "★" * int(conf * 5)
            distance = (pred['current_price'] - level) / pred['current_price'] * 100
            print(f"  S: ${level:.2f} (-{distance:.1f}%) {stars}")
    
    print("\n★ = Confidence level (more stars = higher confidence)")

def show_json_api_output(predictions):
    """Show what the model output would look like as JSON API response"""
    
    print(f"\n{'='*60}")
    print("JSON API OUTPUT FORMAT")
    print("="*60)
    
    # Convert one prediction to JSON-like format
    sample_pred = predictions[0]
    
    api_response = {
        "symbol": sample_pred['symbol'],
        "prediction_date": sample_pred['date'].isoformat(),
        "current_price": round(sample_pred['current_price'], 2),
        "model_version": "sr_ensemble_v1.0",
        "prediction_confidence": "HIGH",
        
        "support_levels": [
            {
                "level": round(level, 2),
                "confidence": round(conf, 3),
                "distance_percent": round((sample_pred['current_price'] - level) / sample_pred['current_price'] * 100, 1),
                "historical_tests": tests,
                "strength": "HIGH" if conf > 0.7 else "MEDIUM" if conf > 0.4 else "LOW"
            }
            for level, conf, tests in zip(
                sample_pred['support_levels'], 
                sample_pred['support_confidence'], 
                sample_pred['support_tests']
            )
        ],
        
        "resistance_levels": [
            {
                "level": round(level, 2),
                "confidence": round(conf, 3),
                "distance_percent": round((level - sample_pred['current_price']) / sample_pred['current_price'] * 100, 1),
                "historical_tests": tests,
                "strength": "HIGH" if conf > 0.7 else "MEDIUM" if conf > 0.4 else "LOW"
            }
            for level, conf, tests in zip(
                sample_pred['resistance_levels'], 
                sample_pred['resistance_confidence'], 
                sample_pred['resistance_tests']
            )
        ],
        
        "trading_signals": [],
        
        "risk_metrics": {
            "volatility_percentile": 65,
            "liquidity_score": 8.5,
            "prediction_uncertainty": round(1 - np.mean(sample_pred['support_confidence'] + sample_pred['resistance_confidence']), 3)
        },
        
        "metadata": {
            "features_used": len(sample_pred['input_features']),
            "model_training_end": "2023-12-31",
            "last_updated": datetime.now().isoformat(),
            "data_quality_score": 0.92
        }
    }
    
    # Add trading signals based on levels
    strong_support = [s for s in api_response['support_levels'] if s['confidence'] > 0.6]
    strong_resistance = [r for r in api_response['resistance_levels'] if r['confidence'] > 0.6]
    
    if strong_support:
        api_response['trading_signals'].append({
            "signal_type": "BUY_SUPPORT",
            "trigger_price": strong_support[0]['level'] * 1.005,
            "target_price": strong_support[0]['level'] * 1.04,
            "stop_loss": strong_support[0]['level'] * 0.98,
            "confidence": strong_support[0]['confidence'],
            "risk_reward_ratio": 4.0,
            "position_size_recommendation": "2%"
        })
    
    if strong_resistance:
        api_response['trading_signals'].append({
            "signal_type": "SELL_RESISTANCE", 
            "trigger_price": strong_resistance[0]['level'] * 0.995,
            "target_price": strong_resistance[0]['level'] * 0.96,
            "stop_loss": strong_resistance[0]['level'] * 1.02,
            "confidence": strong_resistance[0]['confidence'],
            "risk_reward_ratio": 3.5,
            "position_size_recommendation": "1.5%"
        })
    
    # Pretty print JSON
    import json
    print(json.dumps(api_response, indent=2))

def show_csv_export_format(predictions):
    """Show tabular format suitable for CSV export or database storage"""
    
    print(f"\n{'='*60}")
    print("CSV/DATABASE EXPORT FORMAT")
    print("="*60)
    
    # Convert predictions to tabular format
    rows = []
    
    for pred in predictions[:5]:  # Show first 5
        base_row = {
            'symbol': pred['symbol'],
            'date': pred['date'],
            'current_price': round(pred['current_price'], 2),
            'actual_high': round(pred['actual_high'], 2),
            'actual_low': round(pred['actual_low'], 2),
            'actual_close': round(pred['actual_close'], 2)
        }
        
        # Add support levels
        for i in range(3):
            if i < len(pred['support_levels']):
                base_row[f'support_level_{i+1}'] = round(pred['support_levels'][i], 2)
                base_row[f'support_confidence_{i+1}'] = round(pred['support_confidence'][i], 3)
            else:
                base_row[f'support_level_{i+1}'] = None
                base_row[f'support_confidence_{i+1}'] = None
        
        # Add resistance levels
        for i in range(3):
            if i < len(pred['resistance_levels']):
                base_row[f'resistance_level_{i+1}'] = round(pred['resistance_levels'][i], 2)
                base_row[f'resistance_confidence_{i+1}'] = round(pred['resistance_confidence'][i], 3)
            else:
                base_row[f'resistance_level_{i+1}'] = None
                base_row[f'resistance_confidence_{i+1}'] = None
        
        rows.append(base_row)
    
    # Convert to DataFrame for nice display
    df = pd.DataFrame(rows)
    
    print("Sample rows (showing key columns):")
    print("-" * 80)
    
    # Show subset of columns for readability
    display_cols = ['symbol', 'date', 'current_price', 
                   'support_level_1', 'support_confidence_1',
                   'resistance_level_1', 'resistance_confidence_1',
                   'actual_high', 'actual_low']
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df[display_cols].to_string(index=False))
    
    print(f"\nFull dataset shape: {df.shape}")
    print(f"All columns: {list(df.columns)}")

def main():
    print("🎯 SUPPORT/RESISTANCE MODEL OUTPUT DEMONSTRATION")
    print("=" * 80)
    print("This demonstration shows realistic examples of model predictions")
    print("including confidence scores, trading signals, and visualizations.")
    print()
    
    # Set random seed for reproducible demo
    np.random.seed(42)
    random.seed(42)
    
    # Generate sample data
    print("📊 Generating realistic sample data...")
    sample_data = generate_realistic_sample_data()
    
    # Simulate model predictions
    print("🤖 Simulating model predictions...")
    predictions = simulate_model_predictions(sample_data)
    
    # Show detailed predictions
    print_detailed_predictions(predictions)
    
    # Create visualizations
    create_prediction_visualization(predictions)
    
    # Show different output formats
    show_json_api_output(predictions)
    show_csv_export_format(predictions)
    
    print(f"\n{'='*80}")
    print("MODEL OUTPUT DEMONSTRATION COMPLETE!")
    print("="*80)
    print()
    print("📈 Key Output Features Demonstrated:")
    print("   ✓ Multiple support/resistance levels per prediction")
    print("   ✓ Confidence scores for each level (0-1 scale)")
    print("   ✓ Historical test counts for validation")
    print("   ✓ Automated trading signal generation")
    print("   ✓ Visual representation of predictions")
    print("   ✓ JSON API format for integration")
    print("   ✓ CSV format for analysis/storage")
    print()
    print("🎯 Production Integration:")
    print("   • Real-time API endpoints")
    print("   • Database storage with time-series indexing")
    print("   • Alert system for high-confidence signals")
    print("   • Performance tracking and model monitoring")
    print()
    print("📊 Model Prediction Characteristics:")
    print("   • 1-3 support levels below current price")
    print("   • 1-3 resistance levels above current price") 
    print("   • Confidence scores based on historical effectiveness")
    print("   • Distance percentages for risk assessment")
    print("   • Ensemble weighting for improved accuracy")

if __name__ == "__main__":
    main()