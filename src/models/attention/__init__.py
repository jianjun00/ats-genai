#!/usr/bin/env python3
"""
Attention Mechanisms for Multi-Scale Financial Time Series

This module provides advanced attention mechanisms for financial time series modeling,
including cross-scale attention, hierarchical processing, and interpretable patterns.

Key Components:
- CrossScaleAttention: Multi-temporal scale attention
- HierarchicalPositionalEncoding: Scale-aware position encoding
- RelativePositionBias: Relative position attention bias
- ScaleSpecificAttention: Individual scale attention
- CrossScaleFusion: Multi-scale output fusion
"""

from .cross_scale_attention import (
    CrossScaleAttention,
    HierarchicalPositionalEncoding,
    RelativePositionBias,
    ScaleSpecificAttention,
    CrossScaleFusion,
    AttentionConfig,
    create_cross_scale_attention
)

__all__ = [
    'CrossScaleAttention',
    'HierarchicalPositionalEncoding', 
    'RelativePositionBias',
    'ScaleSpecificAttention',
    'CrossScaleFusion',
    'AttentionConfig',
    'create_cross_scale_attention'
]