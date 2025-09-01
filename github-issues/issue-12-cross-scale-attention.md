# Issue #12: Cross-Scale Attention Mechanism

## 📋 Summary
Implement a Cross-Scale Attention mechanism that enables the model to attend to patterns across different temporal scales (minute, hourly, daily) simultaneously, inspired by hierarchical attention research in 2024-2025.

## 🎯 Objectives
- [ ] Create CrossScaleAttention module for multi-temporal processing
- [ ] Implement hierarchical attention across time scales
- [ ] Add positional encoding for different time scales
- [ ] Integrate with existing TFT architecture
- [ ] Provide attention visualization capabilities

## 🔧 Technical Requirements

### Cross-Scale Attention Architecture
```python
class CrossScaleAttention(nn.Module):
    """Multi-scale temporal attention mechanism"""
    
    def __init__(self, d_model: int = 64, n_scales: int = 3, n_heads: int = 4):
        self.d_model = d_model
        self.n_scales = n_scales
        self.n_heads = n_heads
        
        # Scale-specific projections
        self.scale_projections = nn.ModuleList([
            nn.Linear(d_model, d_model) for _ in range(n_scales)
        ])
        
        # Multi-head attention for each scale
        self.scale_attention = nn.ModuleList([
            nn.MultiheadAttention(d_model, n_heads, batch_first=True)
            for _ in range(n_scales)
        ])
        
        # Cross-scale fusion
        self.cross_scale_fusion = CrossScaleFusion(d_model, n_scales)
        
    def forward(self, minute_features, hourly_features, daily_features):
        scale_features = [minute_features, hourly_features, daily_features]
        scale_outputs = []
        attention_weights = []
        
        # Process each scale
        for i, (features, projection, attention) in enumerate(
            zip(scale_features, self.scale_projections, self.scale_attention)
        ):
            # Project to common dimension
            projected = projection(features)
            
            # Self-attention within scale
            attended, attn_weights = attention(projected, projected, projected)
            scale_outputs.append(attended)
            attention_weights.append(attn_weights)
        
        # Cross-scale fusion
        fused_output = self.cross_scale_fusion(scale_outputs)
        
        return fused_output, attention_weights
```

### Hierarchical Positional Encoding
```python
class HierarchicalPositionalEncoding(nn.Module):
    """Positional encoding that handles multiple time scales"""
    
    def __init__(self, d_model: int, max_len: int = 10000):
        super().__init__()
        self.d_model = d_model
        
        # Different positional encodings for different scales
        self.minute_encoding = self._create_positional_encoding(max_len, d_model)
        self.hourly_encoding = self._create_positional_encoding(max_len // 60, d_model)
        self.daily_encoding = self._create_positional_encoding(max_len // 1440, d_model)
    
    def forward(self, features, scale: str):
        if scale == 'minute':
            return features + self.minute_encoding[:features.size(1)]
        elif scale == 'hourly':
            return features + self.hourly_encoding[:features.size(1)]
        elif scale == 'daily':
            return features + self.daily_encoding[:features.size(1)]
        else:
            return features
```

## 📁 File Structure
```
src/models/attention/
├── cross_scale_attention.py        # Main cross-scale attention
├── hierarchical_positional.py      # Multi-scale positional encoding
├── scale_fusion.py                 # Cross-scale fusion mechanisms
└── attention_visualization.py      # Attention pattern visualization

src/models/
├── enhanced_tft.py                 # TFT with cross-scale attention
└── multi_scale_transformer.py     # Complete multi-scale model

tests/models/attention/
├── test_cross_scale_attention.py
├── test_hierarchical_positional.py
└── test_scale_fusion.py
```

## 🧪 Acceptance Criteria
- [ ] Cross-scale attention processes minute/hourly/daily features
- [ ] Hierarchical positional encoding works across scales
- [ ] Attention weights provide interpretable cross-scale patterns
- [ ] Integration with TFT maintains or improves performance
- [ ] Memory efficient implementation for long sequences
- [ ] Visualization tools for attention analysis

## 🔗 Dependencies
- [ ] torch (existing)
- [ ] matplotlib (for attention visualization)
- [ ] seaborn (for heatmap visualization)

## 📊 Performance Targets
- Cross-scale attention computation: <100ms per batch
- Memory usage increase: <30% over single-scale attention
- Attention pattern interpretability: Clear scale-specific patterns
- Model performance: ≥5% improvement in prediction accuracy

## 🏷️ Labels
`enhancement`, `ml-models`, `attention`, `phase-1`

## 👥 Assignee
ML Team

## 🕒 Timeline
**Sprint 1** (Week 1-2)
- Design cross-scale attention architecture
- Implement hierarchical positional encoding
- Basic unit tests

**Sprint 2** (Week 3-4)
- Cross-scale fusion mechanisms
- TFT integration and testing
- Attention visualization tools

---
**Priority:** Medium-High  
**Complexity:** High  
**Phase:** 1