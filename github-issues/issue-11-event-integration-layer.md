# Issue #11: Event Integration Layer for TFT Models

## 📋 Summary
Implement an Event Integration Layer that incorporates news, earnings, upgrades, and other market events into the existing Temporal Fusion Transformer model, following 2024-2025 research on LLM-based event analysis.

## 🎯 Objectives
- [ ] Create EventIntegrationLayer for TFT models
- [ ] Implement time-aware event attention mechanism
- [ ] Add support for news, earnings, and upgrade events
- [ ] Integrate with existing sentiment analysis features
- [ ] Provide interpretable event impact scoring

## 🔧 Technical Requirements

### Event Integration Architecture
```python
class EventIntegrationLayer(nn.Module):
    """Time-aware event integration for sequence models"""
    
    def __init__(self, d_model: int = 64, event_dim: int = 32):
        self.event_encoder = EventEncoder(event_dim, d_model)
        self.temporal_attention = TemporalEventAttention(d_model)
        self.event_gate = EventGatingMechanism(d_model)
    
    def forward(self, sequence_features, events, event_timestamps):
        # Encode events with temporal positioning
        event_embeddings = self.event_encoder(events, event_timestamps)
        
        # Time-aware attention between sequence and events
        event_context = self.temporal_attention(sequence_features, event_embeddings)
        
        # Gated integration
        enhanced_features = self.event_gate(sequence_features, event_context)
        
        return enhanced_features, event_attention_weights
```

### Event Data Structure
```python
@dataclass
class MarketEvent:
    event_id: str
    symbol: str
    timestamp: datetime
    event_type: str  # 'news', 'earnings', 'upgrade', 'economic'
    content: str
    sentiment_score: float
    importance_score: float
    embedding: Optional[np.ndarray] = None
    
@dataclass  
class EventSequence:
    events: List[MarketEvent]
    time_range: Tuple[datetime, datetime]
    event_index: Dict[datetime, List[int]]  # Fast temporal lookup
```

## 📁 File Structure
```
src/events/
├── event_integration.py           # Main integration layer
├── event_encoder.py              # Event encoding module
├── temporal_attention.py         # Time-aware attention
├── event_data_structures.py      # Event data models
└── event_preprocessing.py        # Event data preprocessing

src/models/
├── temporal_fusion_transformer.py  # Existing (enhance)
└── enhanced_tft.py                 # TFT with event integration

tests/events/
├── test_event_integration.py
├── test_event_encoder.py
└── test_temporal_attention.py
```

## 🧪 Acceptance Criteria
- [ ] Event integration layer processes news, earnings, upgrades
- [ ] Time-aware attention mechanism working correctly
- [ ] Integration with existing TFT model maintains performance
- [ ] Event impact scores provide interpretability
- [ ] Support for batch processing of event sequences
- [ ] Memory efficient event caching and indexing

## 🔗 Dependencies  
- [ ] transformers (for event encoding)
- [ ] sentence-transformers (for event embeddings)
- [ ] existing sentiment analysis module

## 📊 Performance Targets
- Event encoding: <10ms per event batch
- Event attention computation: <50ms per sequence
- Memory overhead: <20% increase over base TFT
- Event impact interpretability: >0.8 correlation with manual analysis

## 🏷️ Labels
`enhancement`, `ml-models`, `events`, `phase-1`

## 👥 Assignee
ML Team

## 🕒 Timeline
**Sprint 1** (Week 1-2)
- Design event data structures
- Implement EventEncoder and basic integration
- Unit tests for core components

**Sprint 2** (Week 3-4)
- Temporal attention mechanisms
- TFT integration and testing
- Performance optimization

---
**Priority:** High  
**Complexity:** Medium-High  
**Phase:** 1