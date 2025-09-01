#!/usr/bin/env python3
"""
Tests for Cross-Scale Attention Mechanism

Comprehensive tests for cross-scale attention components including
hierarchical positional encoding, scale-specific attention, and fusion mechanisms.
"""

import pytest
import torch
import torch.nn as nn
import numpy as np
import math
from typing import Dict

from src.models.attention.cross_scale_attention import (
    CrossScaleAttention,
    HierarchicalPositionalEncoding,
    RelativePositionBias,
    ScaleSpecificAttention,
    CrossScaleFusion,
    AttentionConfig,
    create_cross_scale_attention
)
from src.storage.multi_scale_sequence import TimeScale


class TestAttentionConfig:
    """Test AttentionConfig functionality."""
    
    def test_default_config(self):
        """Test default attention configuration."""
        config = AttentionConfig()
        
        assert config.d_model == 64
        assert config.n_heads == 4
        assert config.n_scales == 3
        assert config.dropout == 0.1
        assert config.temperature == 1.0
        assert config.use_relative_position is True
        assert config.max_relative_position == 512
        assert config.attention_window is None
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = AttentionConfig(
            d_model=128,
            n_heads=8,
            dropout=0.2,
            attention_window=256
        )
        
        assert config.d_model == 128
        assert config.n_heads == 8
        assert config.dropout == 0.2
        assert config.attention_window == 256


class TestHierarchicalPositionalEncoding:
    """Test HierarchicalPositionalEncoding functionality."""
    
    def test_hierarchical_encoding_creation(self):
        """Test creation of hierarchical positional encoding."""
        scales = [TimeScale.MINUTE, TimeScale.HOURLY, TimeScale.DAILY]
        encoding = HierarchicalPositionalEncoding(d_model=64, scales=scales)
        
        assert encoding.d_model == 64
        assert len(encoding.scales) == 3
        assert len(encoding.scale_encodings) == 3
        
        # Check that encodings exist for each scale
        for scale in scales:
            assert scale.value in encoding.scale_encodings
            assert encoding.scale_encodings[scale.value].shape == (10000, 64)
    
    def test_scale_factor_computation(self):
        """Test scale factor computation."""
        encoding = HierarchicalPositionalEncoding(d_model=32)
        
        minute_factor = encoding._get_scale_factor(TimeScale.MINUTE)
        hourly_factor = encoding._get_scale_factor(TimeScale.HOURLY)
        daily_factor = encoding._get_scale_factor(TimeScale.DAILY)
        
        assert minute_factor == 1.0
        assert hourly_factor == 60.0
        assert daily_factor == 1440.0
        
        # Higher scale factors should create different frequency patterns
        assert hourly_factor > minute_factor
        assert daily_factor > hourly_factor
    
    def test_positional_encoding_forward(self):
        """Test forward pass of positional encoding."""
        encoding = HierarchicalPositionalEncoding(d_model=32)
        
        batch_size = 2
        seq_len = 50
        x = torch.randn(batch_size, seq_len, 32)
        
        # Test with minute scale
        encoded = encoding(x, TimeScale.MINUTE)
        
        assert encoded.shape == (batch_size, seq_len, 32)
        assert not torch.isnan(encoded).any()
        
        # Should not be identical to input (position added)
        assert not torch.equal(encoded, x)
    
    def test_positional_encoding_with_positions(self):
        """Test encoding with explicit positions."""
        encoding = HierarchicalPositionalEncoding(d_model=16)
        
        batch_size = 1
        seq_len = 10
        x = torch.randn(batch_size, seq_len, 16)
        positions = torch.arange(0, seq_len).unsqueeze(0)
        
        encoded = encoding(x, TimeScale.HOURLY, positions)
        
        assert encoded.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(encoded).any()
    
    def test_different_scales_produce_different_encodings(self):
        """Test that different scales produce different encodings."""
        encoding = HierarchicalPositionalEncoding(d_model=32)
        
        batch_size = 1
        seq_len = 20
        x = torch.zeros(batch_size, seq_len, 32)  # Use zeros to isolate position effect
        
        minute_encoded = encoding(x, TimeScale.MINUTE)
        hourly_encoded = encoding(x, TimeScale.HOURLY)
        daily_encoded = encoding(x, TimeScale.DAILY)
        
        # Different scales should produce different encodings
        assert not torch.equal(minute_encoded, hourly_encoded)
        assert not torch.equal(hourly_encoded, daily_encoded)
        assert not torch.equal(minute_encoded, daily_encoded)


class TestRelativePositionBias:
    """Test RelativePositionBias functionality."""
    
    def test_relative_position_bias_creation(self):
        """Test creation of relative position bias."""
        bias = RelativePositionBias(num_heads=4, max_relative_position=128)
        
        assert bias.num_heads == 4
        assert bias.max_relative_position == 128
        
        # Bias table should have correct shape
        expected_size = 2 * 128 - 1  # 2 * max_relative_position - 1
        assert bias.relative_position_bias_table.shape == (expected_size, 4)
    
    def test_relative_position_bias_forward(self):
        """Test relative position bias computation."""
        bias = RelativePositionBias(num_heads=2, max_relative_position=16)
        
        seq_len = 10
        bias_matrix = bias(seq_len)
        
        assert bias_matrix.shape == (2, seq_len, seq_len)  # num_heads, seq_len, seq_len
        assert not torch.isnan(bias_matrix).any()
        
        # Bias should be symmetric around diagonal
        for h in range(2):
            for i in range(seq_len):
                for j in range(seq_len):
                    relative_pos_ij = i - j
                    relative_pos_ji = j - i
                    
                    # Should have same bias for same relative distance
                    if abs(relative_pos_ij) < 16:  # Within max range
                        assert bias_matrix[h, i, j] == bias_matrix[h, j, i]
    
    def test_bias_clamping(self):
        """Test that relative positions are properly clamped."""
        bias = RelativePositionBias(num_heads=1, max_relative_position=5)
        
        # Test with sequence longer than max relative position
        seq_len = 20
        bias_matrix = bias(seq_len)
        
        assert bias_matrix.shape == (1, seq_len, seq_len)
        assert not torch.isnan(bias_matrix).any()
        
        # Very distant positions should have same bias (clamped)
        assert bias_matrix[0, 0, 19] == bias_matrix[0, 0, 10]  # Both clamped to max


class TestScaleSpecificAttention:
    """Test ScaleSpecificAttention functionality."""
    
    def test_scale_attention_creation(self):
        """Test creation of scale-specific attention."""
        attention = ScaleSpecificAttention(
            d_model=64,
            num_heads=4,
            dropout=0.1,
            attention_window=None,
            use_relative_position=True
        )
        
        assert attention.d_model == 64
        assert attention.num_heads == 4
        assert attention.head_dim == 16  # d_model // num_heads
        assert attention.relative_position_bias is not None
    
    def test_scale_attention_forward(self):
        """Test forward pass of scale-specific attention."""
        attention = ScaleSpecificAttention(d_model=32, num_heads=2)
        
        batch_size = 2
        seq_len = 20
        
        query = torch.randn(batch_size, seq_len, 32)
        key = torch.randn(batch_size, seq_len, 32)
        value = torch.randn(batch_size, seq_len, 32)
        
        output = attention(query, key, value)
        
        assert output.shape == (batch_size, seq_len, 32)
        assert not torch.isnan(output).any()
        
        # Output should be different from input (attention applied)
        assert not torch.equal(output, query)
    
    def test_scale_attention_with_mask(self):
        """Test attention with attention mask."""
        attention = ScaleSpecificAttention(d_model=16, num_heads=2)
        
        batch_size = 1
        seq_len = 10
        
        query = torch.randn(batch_size, seq_len, 16)
        key = torch.randn(batch_size, seq_len, 16)
        value = torch.randn(batch_size, seq_len, 16)
        
        # Create mask that blocks last 5 positions
        attention_mask = torch.ones(batch_size, seq_len)
        attention_mask[:, 5:] = 0
        
        output = attention(query, key, value, attention_mask)
        
        assert output.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(output).any()
    
    def test_attention_with_different_key_value_length(self):
        """Test attention with different key/value sequence length."""
        attention = ScaleSpecificAttention(d_model=16, num_heads=2, use_relative_position=False)
        
        batch_size = 1
        query_len = 10
        key_len = 15
        
        query = torch.randn(batch_size, query_len, 16)
        key = torch.randn(batch_size, key_len, 16)
        value = torch.randn(batch_size, key_len, 16)
        
        output = attention(query, key, value)
        
        assert output.shape == (batch_size, query_len, 16)
        assert not torch.isnan(output).any()
    
    def test_return_attention_weights(self):
        """Test returning attention weights."""
        attention = ScaleSpecificAttention(d_model=16, num_heads=2)
        
        batch_size = 1
        seq_len = 8
        
        query = torch.randn(batch_size, seq_len, 16)
        key = torch.randn(batch_size, seq_len, 16)
        value = torch.randn(batch_size, seq_len, 16)
        
        output, weights = attention(query, key, value, return_attention=True)
        
        assert output.shape == (batch_size, seq_len, 16)
        assert weights.shape == (batch_size, seq_len, seq_len)
        assert not torch.isnan(output).any()
        assert not torch.isnan(weights).any()
        
        # Attention weights should sum to 1 along last dimension
        assert torch.allclose(weights.sum(dim=-1), torch.ones(batch_size, seq_len), atol=1e-5)
    
    def test_local_attention_window(self):
        """Test local attention with window."""
        attention = ScaleSpecificAttention(
            d_model=16, num_heads=2, attention_window=4, use_relative_position=False
        )
        
        batch_size = 1
        seq_len = 10
        
        query = torch.randn(batch_size, seq_len, 16)
        key = torch.randn(batch_size, seq_len, 16)
        value = torch.randn(batch_size, seq_len, 16)
        
        output, weights = attention(query, key, value, return_attention=True)
        
        assert output.shape == (batch_size, seq_len, 16)
        assert weights.shape == (batch_size, seq_len, seq_len)
        
        # Check that attention is indeed local (should be zero outside window)
        # For position i, only positions max(0, i-2) to min(seq_len, i+2) should have attention
        for i in range(seq_len):
            for j in range(seq_len):
                if abs(i - j) > 2:  # Outside window of size 4 (±2)
                    assert abs(weights[0, i, j].item()) < 1e-6


class TestCrossScaleFusion:
    """Test CrossScaleFusion functionality."""
    
    def test_fusion_creation(self):
        """Test creation of cross-scale fusion."""
        fusion = CrossScaleFusion(d_model=32, num_scales=3, fusion_method='learned_weighted')
        
        assert fusion.d_model == 32
        assert fusion.num_scales == 3
        assert fusion.fusion_method == 'learned_weighted'
        assert hasattr(fusion, 'fusion_weights')
    
    def test_learned_weighted_fusion(self):
        """Test learned weighted fusion."""
        fusion = CrossScaleFusion(d_model=16, num_scales=3, fusion_method='learned_weighted')
        
        batch_size = 2
        seq_len = 10
        
        scale_outputs = [
            torch.randn(batch_size, seq_len, 16),
            torch.randn(batch_size, seq_len, 16),
            torch.randn(batch_size, seq_len, 16)
        ]
        
        fused = fusion(scale_outputs)
        
        assert fused.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(fused).any()
        
        # Should be different from all individual inputs
        for output in scale_outputs:
            assert not torch.equal(fused, output)
    
    def test_attention_based_fusion(self):
        """Test attention-based fusion."""
        fusion = CrossScaleFusion(d_model=16, num_scales=2, fusion_method='attention_based')
        
        batch_size = 1
        seq_len = 8
        
        scale_outputs = [
            torch.randn(batch_size, seq_len, 16),
            torch.randn(batch_size, seq_len, 16)
        ]
        
        fused = fusion(scale_outputs)
        
        assert fused.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(fused).any()
    
    def test_gated_fusion(self):
        """Test gated fusion."""
        fusion = CrossScaleFusion(d_model=16, num_scales=2, fusion_method='gated')
        
        batch_size = 1
        seq_len = 5
        
        scale_outputs = [
            torch.randn(batch_size, seq_len, 16),
            torch.randn(batch_size, seq_len, 16)
        ]
        
        fused = fusion(scale_outputs)
        
        assert fused.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(fused).any()
    
    def test_single_scale_fusion(self):
        """Test fusion with single scale (should just normalize)."""
        fusion = CrossScaleFusion(d_model=16, num_scales=1, fusion_method='learned_weighted')
        
        batch_size = 1
        seq_len = 5
        
        scale_outputs = [torch.randn(batch_size, seq_len, 16)]
        
        fused = fusion(scale_outputs)
        
        assert fused.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(fused).any()
    
    def test_empty_scale_outputs(self):
        """Test fusion with empty scale outputs."""
        fusion = CrossScaleFusion(d_model=16, num_scales=2)
        
        with pytest.raises(ValueError, match="No scale outputs provided"):
            fusion([])


class TestCrossScaleAttention:
    """Test complete CrossScaleAttention functionality."""
    
    def test_cross_scale_attention_creation(self):
        """Test creation of cross-scale attention."""
        config = AttentionConfig(d_model=32, n_heads=2, n_scales=3)
        attention = CrossScaleAttention(config)
        
        assert attention.d_model == 32
        assert attention.num_heads == 2
        assert attention.num_scales == 3
        
        assert hasattr(attention, 'positional_encoding')
        assert hasattr(attention, 'scale_attentions')
        assert hasattr(attention, 'fusion_layer')
    
    def test_cross_scale_attention_forward(self):
        """Test forward pass of cross-scale attention."""
        config = AttentionConfig(d_model=16, n_heads=2, n_scales=3)
        attention = CrossScaleAttention(config)
        
        batch_size = 2
        
        scale_features = {
            'minute': torch.randn(batch_size, 60, 16),  # 1 hour of minute data
            'hourly': torch.randn(batch_size, 12, 16),  # 12 hours of hourly data
            'daily': torch.randn(batch_size, 3, 16)     # 3 days of daily data
        }
        
        output = attention(scale_features, primary_scale='minute')
        
        assert output.shape == (batch_size, 60, 16)  # Aligned to primary scale
        assert not torch.isnan(output).any()
        
        # Should be different from input (attention applied)
        assert not torch.equal(output, scale_features['minute'])
    
    def test_cross_scale_with_attention_masks(self):
        """Test cross-scale attention with attention masks."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        scale_features = {
            'minute': torch.randn(batch_size, 30, 16),
            'hourly': torch.randn(batch_size, 6, 16)
        }
        
        attention_masks = {
            'minute': torch.ones(batch_size, 30),
            'hourly': torch.ones(batch_size, 6)
        }
        
        # Mask out last half of minute data
        attention_masks['minute'][:, 15:] = 0
        
        output = attention(scale_features, attention_masks=attention_masks)
        
        assert output.shape == (batch_size, 30, 16)
        assert not torch.isnan(output).any()
    
    def test_return_attention_weights(self):
        """Test returning attention weights."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        scale_features = {
            'minute': torch.randn(batch_size, 20, 16),
            'hourly': torch.randn(batch_size, 4, 16)
        }
        
        output, weights = attention(
            scale_features, return_attention_weights=True
        )
        
        assert output.shape == (batch_size, 20, 16)
        assert isinstance(weights, dict)
        
        # Should have weights for scales that have attention layers
        for scale_name in ['minute', 'hourly']:
            if scale_name in weights:
                assert not torch.isnan(weights[scale_name]).any()
    
    def test_get_attention_patterns(self):
        """Test getting attention patterns for visualization."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        scale_features = {
            'minute': torch.randn(batch_size, 15, 16),
            'hourly': torch.randn(batch_size, 3, 16)
        }
        
        patterns = attention.get_attention_patterns(scale_features)
        
        assert isinstance(patterns, dict)
        
        for scale_name, pattern in patterns.items():
            if pattern is not None:
                assert not torch.isnan(pattern).any()
                # Pattern should be square (self-attention)
                assert pattern.shape[-1] == pattern.shape[-2]
    
    def test_alignment_to_different_lengths(self):
        """Test alignment of features to different lengths."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        # Test alignment to shorter length
        features_long = torch.randn(batch_size, 100, 16)
        aligned_short = attention._align_to_length(features_long, 50)
        assert aligned_short.shape == (batch_size, 50, 16)
        
        # Test alignment to longer length (interpolation)
        features_short = torch.randn(batch_size, 10, 16)
        aligned_long = attention._align_to_length(features_short, 20)
        assert aligned_long.shape == (batch_size, 20, 16)
        
        # Test no alignment needed
        features_same = torch.randn(batch_size, 15, 16)
        aligned_same = attention._align_to_length(features_same, 15)
        assert torch.equal(aligned_same, features_same)
    
    def test_primary_scale_not_found(self):
        """Test error when primary scale not in features."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        scale_features = {
            'hourly': torch.randn(1, 10, 16),
            'daily': torch.randn(1, 5, 16)
        }
        
        with pytest.raises(ValueError, match="Primary scale 'minute' not found"):
            attention(scale_features, primary_scale='minute')


class TestCreateCrossScaleAttention:
    """Test convenience function for creating cross-scale attention."""
    
    def test_create_cross_scale_attention(self):
        """Test creation function."""
        attention = create_cross_scale_attention(
            d_model=64,
            num_heads=4,
            num_scales=3,
            dropout=0.2
        )
        
        assert isinstance(attention, CrossScaleAttention)
        assert attention.config.d_model == 64
        assert attention.config.n_heads == 4
        assert attention.config.n_scales == 3
        assert attention.config.dropout == 0.2


class TestRealisticScenarios:
    """Test realistic multi-scale attention scenarios."""
    
    def test_realistic_financial_data_scales(self):
        """Test with realistic financial data scale relationships."""
        config = AttentionConfig(d_model=32, n_heads=4)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        # Realistic scale relationships
        scale_features = {
            'minute': torch.randn(batch_size, 390, 32),  # 1 trading day (6.5 hours * 60)
            'hourly': torch.randn(batch_size, 24, 32),   # 1 day of hourly data
            'daily': torch.randn(batch_size, 5, 32)      # 1 week of daily data
        }
        
        # Test minute-level prediction with higher-scale context
        output = attention(scale_features, primary_scale='minute')
        
        assert output.shape == (batch_size, 390, 32)
        assert not torch.isnan(output).any()
        
        # Should incorporate information from all scales
        baseline_output = attention.scale_attentions['minute'](
            scale_features['minute'],
            scale_features['minute'],
            scale_features['minute']
        )
        
        # Cross-scale output should be different from single-scale
        assert not torch.equal(output, baseline_output)
    
    def test_intraday_vs_interday_patterns(self):
        """Test attention to intraday vs interday patterns."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        # Create patterns: minute data with intraday pattern, daily with trend
        minute_data = torch.zeros(batch_size, 60, 16)
        # Add intraday volatility pattern (higher in morning/afternoon)
        for t in range(60):
            if t < 20 or t > 40:  # Morning and afternoon sessions
                minute_data[0, t, :] = torch.randn(16) * 2.0  # Higher volatility
            else:
                minute_data[0, t, :] = torch.randn(16) * 0.5  # Lower volatility (lunch)
        
        daily_data = torch.zeros(batch_size, 3, 16)
        # Add trend pattern
        for d in range(3):
            daily_data[0, d, :] = torch.ones(16) * (d + 1)  # Increasing trend
        
        scale_features = {
            'minute': minute_data,
            'daily': daily_data
        }
        
        output = attention(scale_features, primary_scale='minute')
        
        assert output.shape == (batch_size, 60, 16)
        assert not torch.isnan(output).any()
        
        # Output should reflect both intraday patterns and daily trend
        # (This is a qualitative test - in practice you'd check specific patterns)
        assert not torch.equal(output, minute_data)
    
    def test_market_regime_changes(self):
        """Test attention behavior during market regime changes."""
        config = AttentionConfig(d_model=8, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        # Simulate regime change: first half low vol, second half high vol
        minute_data = torch.randn(batch_size, 100, 8)
        minute_data[:, 50:, :] *= 3.0  # Increase volatility in second half
        
        # Daily data shows the regime change
        daily_data = torch.randn(batch_size, 5, 8)
        daily_data[:, 2:, :] *= 2.0  # Regime change on day 3
        
        scale_features = {
            'minute': minute_data,
            'daily': daily_data
        }
        
        output = attention(scale_features, primary_scale='minute')
        
        assert output.shape == (batch_size, 100, 8)
        assert not torch.isnan(output).any()
        
        # Check that the model adapts to regime change
        # (Output characteristics should change between first and second half)
        first_half_var = output[:, :50, :].var()
        second_half_var = output[:, 50:, :].var()
        
        # Some difference expected due to regime change
        assert abs(first_half_var.item() - second_half_var.item()) > 0.01


class TestGradientFlow:
    """Test gradient flow through cross-scale attention."""
    
    def test_gradient_flow(self):
        """Test that gradients flow properly through all components."""
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        scale_features = {
            'minute': torch.randn(batch_size, 20, 16, requires_grad=True),
            'hourly': torch.randn(batch_size, 4, 16, requires_grad=True)
        }
        
        output = attention(scale_features, primary_scale='minute')
        
        # Compute a simple loss
        loss = output.sum()
        loss.backward()
        
        # Check that gradients exist for all inputs
        for scale_name, features in scale_features.items():
            assert features.grad is not None
            assert features.grad.abs().max() > 0
        
        # Check that model parameters have gradients
        for param in attention.parameters():
            if param.requires_grad:
                assert param.grad is not None


class TestMemoryAndPerformance:
    """Test memory usage and performance characteristics."""
    
    def test_memory_efficiency_large_sequences(self):
        """Test memory efficiency with large sequences."""
        config = AttentionConfig(d_model=32, n_heads=4, attention_window=64)
        attention = CrossScaleAttention(config)
        
        batch_size = 1
        
        # Large sequences
        scale_features = {
            'minute': torch.randn(batch_size, 2000, 32),  # ~33 hours of minute data
            'hourly': torch.randn(batch_size, 100, 32),   # ~4 days of hourly data
        }
        
        # Should not run out of memory with local attention
        output = attention(scale_features, primary_scale='minute')
        
        assert output.shape == (batch_size, 2000, 32)
        assert not torch.isnan(output).any()
    
    def test_computational_complexity(self):
        """Test that computational complexity is reasonable."""
        import time
        
        config = AttentionConfig(d_model=16, n_heads=2)
        attention = CrossScaleAttention(config)
        
        # Test with different sequence lengths
        seq_lengths = [50, 100, 200]
        times = []
        
        for seq_len in seq_lengths:
            scale_features = {
                'minute': torch.randn(1, seq_len, 16),
                'hourly': torch.randn(1, seq_len // 4, 16)
            }
            
            start_time = time.time()
            
            with torch.no_grad():
                output = attention(scale_features, primary_scale='minute')
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        # Time should scale reasonably (not exponentially)
        # This is a rough test - exact scaling depends on hardware
        assert all(t < 1.0 for t in times)  # Should complete within 1 second each


if __name__ == "__main__":
    pytest.main([__file__, "-v"])