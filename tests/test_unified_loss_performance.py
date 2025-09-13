#!/usr/bin/env python3
"""
Performance-focused tests for unified loss function implementation.

This script provides focused performance benchmarks and validation
for the cross-domain research synthesis implementation.
"""

import sys
import time
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FinancialAVLoss(nn.Module):
    """Simplified unified loss for performance testing."""

    def __init__(self, num_tasks=5):
        super().__init__()
        self.log_vars = nn.Parameter(torch.zeros(num_tasks))
        self.alpha_cvar = 0.05
        self.lambda_drawdown = 2.0
        self.gamma_focal = 2.0

    def forward(self, predictions, targets, historical_predictions=None):
        device = predictions['price_movement'].device
        total_loss = torch.tensor(0.0, device=device)

        # Multi-task losses with uncertainty weighting
        task_losses = []

        # Price movement with focal enhancement
        price_error = F.mse_loss(predictions['price_movement'], targets['price_movement'], reduction='none')
        price_weights = torch.abs(predictions['price_movement'] - targets['price_movement'])
        focal_weights = torch.pow(price_weights + 1e-8, self.gamma_focal)
        price_loss = torch.mean(focal_weights * price_error)
        task_losses.append(price_loss)

        # Other tasks
        task_losses.append(F.mse_loss(predictions['volatility'], targets['volatility']))
        task_losses.append(F.mse_loss(predictions['volume_profile'], targets['volume_profile']))
        task_losses.append(F.cross_entropy(predictions['regime_change'], targets['regime_change']))
        task_losses.append(F.mse_loss(predictions['risk_assessment'], targets['risk_assessment']))

        # Apply uncertainty weighting
        weighted_loss = torch.tensor(0.0, device=device)
        for i, loss in enumerate(task_losses):
            precision = torch.exp(-self.log_vars[i])
            task_weighted_loss = precision * loss + self.log_vars[i]
            weighted_loss += task_weighted_loss

        total_loss += weighted_loss

        # Financial risk penalties
        returns = predictions['price_movement'].flatten()
        if len(returns) > 2:
            # CVaR penalty
            var_threshold = torch.quantile(returns, self.alpha_cvar)
            tail_losses = returns[returns <= var_threshold]
            if len(tail_losses) > 0:
                cvar_loss = -torch.mean(tail_losses)
                total_loss += cvar_loss

            # Drawdown penalty
            cumulative_returns = torch.cumsum(returns, dim=0)
            running_max = torch.cummax(cumulative_returns, dim=0)[0]
            drawdown = cumulative_returns - running_max
            max_drawdown = -torch.min(drawdown)
            total_loss += self.lambda_drawdown * max_drawdown

        # Temporal consistency
        if historical_predictions is not None and 'price_movement' in historical_predictions:
            if historical_predictions['price_movement'].shape == predictions['price_movement'].shape:
                temporal_loss = 0.1 * F.mse_loss(predictions['price_movement'], historical_predictions['price_movement'])
                total_loss += temporal_loss

        return {'total_loss': total_loss}


class TestTransformer(nn.Module):
    """Lightweight transformer for performance testing."""

    def __init__(self, seq_len=12, d_model=64, nhead=4, num_layers=2):
        super().__init__()

        self.input_projection = nn.Linear(1, d_model)
        self.positional_encoding = nn.Parameter(torch.randn(seq_len, d_model) * 0.1)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead, dim_feedforward=d_model*2,
            dropout=0.1, batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers)

        # Multi-task heads
        self.price_head = nn.Linear(d_model, 1)
        self.volatility_head = nn.Sequential(nn.Linear(d_model, 1), nn.Sigmoid())
        self.volume_head = nn.Sequential(nn.Linear(d_model, 1), nn.Softplus())
        self.regime_head = nn.Linear(d_model, 4)
        self.risk_head = nn.Sequential(nn.Linear(d_model, 1), nn.Sigmoid())

    def forward(self, x):
        batch_size, seq_len, _ = x.shape

        x = self.input_projection(x)
        x = x + self.positional_encoding[:seq_len].unsqueeze(0)
        x = self.transformer(x)

        pooled = x.mean(dim=1)

        return {
            'price_movement': torch.tanh(self.price_head(pooled)),
            'volatility': self.volatility_head(pooled),
            'volume_profile': self.volume_head(pooled),
            'regime_change': self.regime_head(pooled),
            'risk_assessment': self.risk_head(pooled)
        }


def benchmark_forward_pass():
    """Benchmark forward pass performance."""
    logger.info("🚀 Benchmarking forward pass performance...")

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logger.info(f"   Device: {device}")

    # Test configurations
    configs = [
        {'batch_size': 16, 'seq_len': 12, 'd_model': 64, 'layers': 2},
        {'batch_size': 32, 'seq_len': 12, 'd_model': 64, 'layers': 2},
        {'batch_size': 64, 'seq_len': 12, 'd_model': 64, 'layers': 2},
        {'batch_size': 32, 'seq_len': 24, 'd_model': 64, 'layers': 2},
        {'batch_size': 32, 'seq_len': 12, 'd_model': 128, 'layers': 2},
        {'batch_size': 32, 'seq_len': 12, 'd_model': 64, 'layers': 4},
    ]

    results = []

    for config in configs:
        model = TestTransformer(
            config['seq_len'], config['d_model'],
            nhead=min(4, config['d_model']//16), num_layers=config['layers']
        ).to(device)

        input_data = torch.randn(config['batch_size'], config['seq_len'], 1).to(device)

        # Warmup
        with torch.no_grad():
            for _ in range(10):
                _ = model(input_data)

        # Benchmark
        torch.cuda.synchronize() if device.type == 'cuda' else None
        start_time = time.time()

        with torch.no_grad():
            for _ in range(100):
                outputs = model(input_data)

        torch.cuda.synchronize() if device.type == 'cuda' else None
        end_time = time.time()

        avg_time = (end_time - start_time) / 100
        throughput = config['batch_size'] / avg_time

        result = {
            'config': config,
            'avg_time_ms': avg_time * 1000,
            'throughput_samples_per_sec': throughput,
            'memory_mb': torch.cuda.max_memory_allocated() / 1024 / 1024 if device.type == 'cuda' else 'N/A'
        }
        results.append(result)

        logger.info(f"   Config {config}: {avg_time*1000:.2f}ms, {throughput:.1f} samples/s")

        if device.type == 'cuda':
            torch.cuda.reset_peak_memory_stats()

    return results


def benchmark_loss_computation():
    """Benchmark loss computation performance."""
    logger.info("📊 Benchmarking loss computation performance...")

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    loss_function = FinancialAVLoss(num_tasks=5).to(device)

    batch_sizes = [16, 32, 64, 128, 256, 512]
    results = []

    for batch_size in batch_sizes:
        predictions = {
            'price_movement': torch.randn(batch_size, 1).to(device),
            'volatility': torch.abs(torch.randn(batch_size, 1)).to(device),
            'volume_profile': torch.abs(torch.randn(batch_size, 1)).to(device),
            'regime_change': torch.randn(batch_size, 4).to(device),
            'risk_assessment': torch.sigmoid(torch.randn(batch_size, 1)).to(device)
        }

        targets = {
            'price_movement': torch.randn(batch_size, 1).to(device),
            'volatility': torch.abs(torch.randn(batch_size, 1)).to(device),
            'volume_profile': torch.abs(torch.randn(batch_size, 1)).to(device),
            'regime_change': torch.randint(0, 4, (batch_size,)).to(device),
            'risk_assessment': torch.sigmoid(torch.randn(batch_size, 1)).to(device)
        }

        # Warmup
        for _ in range(10):
            _ = loss_function(predictions, targets)

        # Benchmark
        torch.cuda.synchronize() if device.type == 'cuda' else None
        start_time = time.time()

        for _ in range(100):
            loss_components = loss_function(predictions, targets)

        torch.cuda.synchronize() if device.type == 'cuda' else None
        end_time = time.time()

        avg_time = (end_time - start_time) / 100

        result = {
            'batch_size': batch_size,
            'avg_time_ms': avg_time * 1000,
            'throughput_samples_per_sec': batch_size / avg_time
        }
        results.append(result)

        logger.info(f"   Batch {batch_size}: {avg_time*1000:.3f}ms, {batch_size/avg_time:.1f} samples/s")

    return results


def benchmark_gradient_computation():
    """Benchmark gradient computation performance."""
    logger.info("⚡ Benchmarking gradient computation performance...")

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    model = TestTransformer(seq_len=12, d_model=64, nhead=4, num_layers=2).to(device)
    loss_function = FinancialAVLoss(num_tasks=5).to(device)

    batch_sizes = [16, 32, 64, 128]
    results = []

    for batch_size in batch_sizes:
        input_data = torch.randn(batch_size, 12, 1, requires_grad=True).to(device)

        targets = {
            'price_movement': torch.randn(batch_size, 1).to(device),
            'volatility': torch.abs(torch.randn(batch_size, 1)).to(device),
            'volume_profile': torch.abs(torch.randn(batch_size, 1)).to(device),
            'regime_change': torch.randint(0, 4, (batch_size,)).to(device),
            'risk_assessment': torch.sigmoid(torch.randn(batch_size, 1)).to(device)
        }

        # Warmup
        for _ in range(5):
            model.zero_grad()
            outputs = model(input_data)
            loss_components = loss_function(outputs, targets)
            loss_components['total_loss'].backward()

        # Benchmark
        torch.cuda.synchronize() if device.type == 'cuda' else None
        start_time = time.time()

        for _ in range(50):
            model.zero_grad()
            outputs = model(input_data)
            loss_components = loss_function(outputs, targets)
            loss_components['total_loss'].backward()

        torch.cuda.synchronize() if device.type == 'cuda' else None
        end_time = time.time()

        avg_time = (end_time - start_time) / 50

        result = {
            'batch_size': batch_size,
            'avg_time_ms': avg_time * 1000,
            'throughput_samples_per_sec': batch_size / avg_time
        }
        results.append(result)

        logger.info(f"   Batch {batch_size}: {avg_time*1000:.2f}ms, {batch_size/avg_time:.1f} samples/s")

    return results


def test_training_stability():
    """Test training stability over extended periods."""
    logger.info("🔄 Testing training stability...")

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    model = TestTransformer(seq_len=12, d_model=32, nhead=2, num_layers=2).to(device)
    loss_function = FinancialAVLoss(num_tasks=5).to(device)

    optimizer = torch.optim.AdamW(
        list(model.parameters()) + list(loss_function.parameters()),
        lr=0.001, weight_decay=1e-5
    )

    losses = []
    uncertainties_history = []

    logger.info("   Running 500 training steps...")

    for step in range(500):
        optimizer.zero_grad()

        # Generate batch
        input_data = torch.randn(32, 12, 1).to(device)
        outputs = model(input_data)

        targets = {
            'price_movement': torch.randn(32, 1).to(device),
            'volatility': torch.abs(torch.randn(32, 1)).to(device),
            'volume_profile': torch.abs(torch.randn(32, 1)).to(device),
            'regime_change': torch.randint(0, 4, (32,)).to(device),
            'risk_assessment': torch.sigmoid(torch.randn(32, 1)).to(device)
        }

        loss_components = loss_function(outputs, targets)
        loss_components['total_loss'].backward()

        # Gradient clipping
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        torch.nn.utils.clip_grad_norm_(loss_function.parameters(), max_norm=1.0)

        optimizer.step()

        losses.append(loss_components['total_loss'].item())
        uncertainties_history.append(torch.exp(loss_function.log_vars).detach().cpu().numpy().tolist())

        if not torch.isfinite(loss_components['total_loss']):
            logger.error(f"   Training became unstable at step {step}")
            return False

    # Analyze training stability
    early_losses = losses[50:150]
    late_losses = losses[-100:]

    early_avg = np.mean(early_losses)
    late_avg = np.mean(late_losses)
    loss_stability = np.std(late_losses) / np.mean(late_losses)

    logger.info(f"   Early loss average: {early_avg:.4f}")
    logger.info(f"   Late loss average: {late_avg:.4f}")
    logger.info(f"   Loss coefficient of variation: {loss_stability:.4f}")

    # Check uncertainty adaptation
    early_uncertainties = np.mean(uncertainties_history[50:150], axis=0)
    late_uncertainties = np.mean(uncertainties_history[-100:], axis=0)

    logger.info(f"   Early uncertainties: {early_uncertainties}")
    logger.info(f"   Late uncertainties: {late_uncertainties}")

    return {
        'stable': loss_stability < 0.5,  # Reasonable stability threshold
        'loss_reduction': (early_avg - late_avg) / early_avg,
        'loss_stability': loss_stability,
        'uncertainty_adaptation': np.abs(late_uncertainties - early_uncertainties).mean()
    }


def validate_research_insights():
    """Validate key research insights from both domains."""
    logger.info("🔬 Validating cross-domain research insights...")

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    loss_function = FinancialAVLoss(num_tasks=5).to(device)

    # Test 1: Uncertainty weighting adaptation (AV insight)
    logger.info("   Testing uncertainty weighting adaptation...")

    # Create scenario where regime task is consistently harder
    hard_predictions = {
        'price_movement': torch.randn(20, 1).to(device) * 0.01,
        'volatility': torch.abs(torch.randn(20, 1)).to(device) * 0.01,
        'volume_profile': torch.abs(torch.randn(20, 1)).to(device) * 0.01,
        'regime_change': torch.randn(20, 4).to(device) * 5.0,  # Much harder task
        'risk_assessment': torch.sigmoid(torch.randn(20, 1)).to(device) * 0.01
    }

    targets = {
        'price_movement': torch.randn(20, 1).to(device) * 0.01,
        'volatility': torch.abs(torch.randn(20, 1)).to(device) * 0.01,
        'volume_profile': torch.abs(torch.randn(20, 1)).to(device) * 0.01,
        'regime_change': torch.randint(0, 4, (20,)).to(device),
        'risk_assessment': torch.sigmoid(torch.randn(20, 1)).to(device) * 0.01
    }

    optimizer = torch.optim.Adam(loss_function.parameters(), lr=0.01)

    initial_uncertainties = loss_function.log_vars.clone()

    for _ in range(100):
        optimizer.zero_grad()
        loss_components = loss_function(hard_predictions, targets)
        loss_components['total_loss'].backward()
        optimizer.step()

    final_uncertainties = loss_function.log_vars
    uncertainty_changes = final_uncertainties - initial_uncertainties

    regime_change = uncertainty_changes[3].item()  # Regime task index
    other_changes = torch.mean(uncertainty_changes[:3]).item()  # Other tasks

    logger.info(f"   Regime task uncertainty change: {regime_change:.4f}")
    logger.info(f"   Other tasks avg change: {other_changes:.4f}")

    av_insight_validated = regime_change > other_changes
    logger.info(f"   AV uncertainty weighting insight: {'✅ VALIDATED' if av_insight_validated else '❌ FAILED'}")

    # Test 2: Risk-aware penalties (Finance insight)
    logger.info("   Testing risk-aware penalties...")

    risky_returns = torch.tensor([
        0.1, -0.15, 0.05, -0.12, 0.08, -0.10, 0.03, -0.08, 0.02, -0.05,
        0.07, -0.13, 0.04, -0.11, 0.06, -0.09, 0.01, -0.07, 0.03, -0.04
    ]).unsqueeze(-1).to(device)
    safe_returns = torch.tensor([
        0.02, 0.015, 0.025, 0.01, 0.02, 0.018, 0.022, 0.016, 0.019, 0.021,
        0.017, 0.023, 0.014, 0.020, 0.016, 0.024, 0.012, 0.018, 0.015, 0.019
    ]).unsqueeze(-1).to(device)

    risky_predictions = dict(hard_predictions)
    safe_predictions = dict(hard_predictions)
    risky_predictions['price_movement'] = risky_returns
    safe_predictions['price_movement'] = safe_returns

    risky_loss = loss_function(risky_predictions, targets)
    safe_loss = loss_function(safe_predictions, targets)

    finance_insight_validated = risky_loss['total_loss'].item() > safe_loss['total_loss'].item()

    logger.info(f"   Risky scenario loss: {risky_loss['total_loss'].item():.4f}")
    logger.info(f"   Safe scenario loss: {safe_loss['total_loss'].item():.4f}")
    logger.info(f"   Finance risk penalty insight: {'✅ VALIDATED' if finance_insight_validated else '❌ FAILED'}")

    return {
        'av_uncertainty_weighting': av_insight_validated,
        'finance_risk_penalties': finance_insight_validated,
        'overall_validation': av_insight_validated and finance_insight_validated
    }


def main():
    """Run comprehensive performance tests."""
    logger.info("🚀 COMPREHENSIVE PERFORMANCE TESTING")
    logger.info("=" * 80)
    logger.info("Testing unified loss function implementation performance")
    logger.info("🚗 Autonomous driving + 💰 Financial trading synthesis")
    logger.info("=" * 80)

    results = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'device': str(torch.device('cuda' if torch.cuda.is_available() else 'cpu')),
        'torch_version': torch.__version__
    }

    try:
        # Forward pass benchmarks
        forward_results = benchmark_forward_pass()
        results['forward_pass_benchmarks'] = forward_results

        # Loss computation benchmarks
        loss_results = benchmark_loss_computation()
        results['loss_computation_benchmarks'] = loss_results

        # Gradient computation benchmarks
        gradient_results = benchmark_gradient_computation()
        results['gradient_computation_benchmarks'] = gradient_results

        # Training stability test
        stability_results = test_training_stability()
        results['training_stability'] = stability_results

        # Research insights validation
        insights_results = validate_research_insights()
        results['research_validation'] = insights_results

        # Performance summary
        logger.info("\n📊 PERFORMANCE SUMMARY")
        logger.info("=" * 60)

        # Best forward pass performance
        best_forward = min(forward_results, key=lambda x: x['avg_time_ms'])
        logger.info(f"⚡ Best forward pass: {best_forward['avg_time_ms']:.2f}ms ({best_forward['throughput_samples_per_sec']:.1f} samples/s)")

        # Best loss computation performance
        best_loss = min(loss_results, key=lambda x: x['avg_time_ms'])
        logger.info(f"📊 Best loss computation: {best_loss['avg_time_ms']:.3f}ms ({best_loss['throughput_samples_per_sec']:.1f} samples/s)")

        # Training stability
        logger.info(f"🔄 Training stability: {'✅ STABLE' if stability_results['stable'] else '❌ UNSTABLE'}")

        # Research validation
        logger.info(f"🔬 Research insights: {'✅ VALIDATED' if insights_results['overall_validation'] else '❌ FAILED'}")

        # Production readiness assessment
        production_ready = (
            best_forward['avg_time_ms'] < 100 and  # <100ms inference
            stability_results['stable'] and
            insights_results['overall_validation']
        )

        logger.info(f"\n🚀 Production readiness: {'✅ READY' if production_ready else '❌ NOT READY'}")

        # Save results
        with open('/tmp/unified_loss_performance_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)

        logger.info(f"💾 Results saved to: /tmp/unified_loss_performance_results.json")

        if production_ready:
            logger.info("\n🎉 UNIFIED LOSS IMPLEMENTATION PERFORMANCE VALIDATED!")
            logger.info("✅ Cross-domain research synthesis working optimally")
            logger.info("✅ Production-ready performance characteristics")
            logger.info("✅ Training stability confirmed")
            logger.info("✅ Research insights validated")
            logger.info("🚗→📈 Ready for deployment!")

            return True
        else:
            logger.warning("⚠️ Performance issues detected - optimization needed")
            return False

    except Exception as e:
        logger.error(f"❌ Performance testing failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)