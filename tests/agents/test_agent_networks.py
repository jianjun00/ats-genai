#!/usr/bin/env python3
"""
Comprehensive Tests for Agent Interaction Networks

Tests multi-agent systems for stock modeling including:
- Individual agent behavior and learning
- Multi-agent communication and coordination
- Graph-based attention mechanisms
- Portfolio optimization through agent networks
- Feature flag integration and graceful degradation
"""

import pytest
import torch
import torch.nn as nn
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

# Test imports - would normally be handled by pytest fixtures
import sys
sys.path.insert(0, 'src')

from shared.utils.feature_flags import FeatureManager, feature_manager
from agents.agent_networks import (
    StockAgent, AgentConfig, NetworkConfig, AgentInteractionNetwork,
    GraphAttentionNetwork, PortfolioAgentSystem, create_agent_network,
    create_portfolio_system
)


class TestAgentConfig:
    """Test agent configuration."""

    def test_agent_config_creation(self):
        """Test basic agent config creation."""
        config = AgentConfig(
            agent_id="test_agent",
            stock_symbol="AAPL",
            hidden_dim=128,
            risk_tolerance=0.3
        )

        assert config.agent_id == "test_agent"
        assert config.stock_symbol == "AAPL"
        assert config.hidden_dim == 128
        assert config.risk_tolerance == 0.3
        assert config.action_dim == 3  # Default

    def test_agent_config_defaults(self):
        """Test agent config with default values."""
        config = AgentConfig(
            agent_id="test_agent",
            stock_symbol="MSFT"
        )

        assert config.hidden_dim == 256
        assert config.action_dim == 3
        assert config.learning_rate == 0.001
        assert config.memory_horizon == 60
        assert config.interaction_radius == 0.1


class TestStockAgent:
    """Test individual stock agent behavior."""

    @pytest.fixture
    def agent_config(self):
        """Create test agent configuration."""
        return AgentConfig(
            agent_id="test_agent_aapl",
            stock_symbol="AAPL",
            hidden_dim=64  # Smaller for testing
        )

    @pytest.fixture
    def stock_agent(self, agent_config):
        """Create test stock agent."""
        return StockAgent(agent_config)

    def test_agent_initialization(self, stock_agent, agent_config):
        """Test agent initialization."""
        assert stock_agent.config == agent_config
        assert hasattr(stock_agent, 'state_encoder')
        assert hasattr(stock_agent, 'action_head')
        assert hasattr(stock_agent, 'value_head')
        assert stock_agent.memory.shape == (60, 64)  # memory_horizon x hidden_dim

    def test_agent_forward_pass(self, stock_agent):
        """Test agent forward pass."""
        batch_size = 1
        hidden_dim = 64
        market_features = torch.randn(batch_size, hidden_dim)

        actions, values, messages = stock_agent(market_features)

        assert actions.shape == (batch_size, 3)  # 3 actions: buy, hold, sell
        assert values.shape == (batch_size, 1)
        assert messages.shape == (batch_size, hidden_dim)

        # Actions should be probabilities (sum to 1)
        assert torch.allclose(actions.sum(dim=-1), torch.ones(batch_size), atol=1e-6)

    def test_agent_memory_update(self, stock_agent):
        """Test agent memory management."""
        hidden_dim = 64

        # Initial memory should be zeros
        assert torch.allclose(stock_agent.memory, torch.zeros(60, hidden_dim))

        # Update memory with test state
        test_state = torch.randn(hidden_dim)
        stock_agent.update_memory(test_state)

        # First memory slot should contain test state
        assert torch.allclose(stock_agent.memory[0], test_state)

        # Memory index should advance
        assert stock_agent.memory_idx == 1

    def test_agent_message_processing(self, stock_agent):
        """Test agent message processing from other agents."""
        hidden_dim = 64
        num_messages = 3

        # Create test messages
        messages = torch.randn(num_messages, hidden_dim)
        sender_masks = torch.ones(num_messages, dtype=torch.bool)

        processed_message = stock_agent.process_messages(messages, sender_masks)

        assert processed_message.shape == (hidden_dim,)

        # Test with empty messages
        empty_messages = torch.empty(0, hidden_dim)
        empty_masks = torch.empty(0, dtype=torch.bool)

        processed_empty = stock_agent.process_messages(empty_messages, empty_masks)
        assert processed_empty.shape == (hidden_dim,)

    def test_agent_with_messages(self, stock_agent):
        """Test agent forward pass with incoming messages."""
        batch_size = 1
        hidden_dim = 64
        num_messages = 2

        market_features = torch.randn(batch_size, hidden_dim)
        messages = torch.randn(num_messages, hidden_dim)
        sender_masks = torch.ones(num_messages, dtype=torch.bool)

        actions, values, out_messages = stock_agent(market_features, messages, sender_masks)

        assert actions.shape == (batch_size, 3)
        assert values.shape == (batch_size, 1)
        assert out_messages.shape == (batch_size, hidden_dim)


class TestGraphAttentionNetwork:
    """Test graph attention network for agent interactions."""

    @pytest.fixture
    def graph_attention(self):
        """Create test graph attention network."""
        return GraphAttentionNetwork(hidden_dim=64, num_heads=4)

    def test_graph_attention_initialization(self, graph_attention):
        """Test graph attention initialization."""
        assert graph_attention.hidden_dim == 64
        assert graph_attention.num_heads == 4
        assert hasattr(graph_attention, 'attention')
        assert hasattr(graph_attention, 'norm')
        assert hasattr(graph_attention, 'ffn')

    def test_graph_attention_forward(self, graph_attention):
        """Test graph attention forward pass."""
        num_agents = 5
        hidden_dim = 64

        agent_states = torch.randn(num_agents, hidden_dim)
        adjacency_mask = torch.ones(num_agents, num_agents, dtype=torch.bool)
        # Remove self-connections
        adjacency_mask.fill_diagonal_(False)

        updated_states = graph_attention(agent_states, adjacency_mask)

        assert updated_states.shape == (num_agents, hidden_dim)

        # States should be different after attention
        assert not torch.allclose(agent_states, updated_states)

    def test_graph_attention_sparse_connectivity(self, graph_attention):
        """Test graph attention with sparse connectivity."""
        num_agents = 4
        hidden_dim = 64

        agent_states = torch.randn(num_agents, hidden_dim)

        # Create sparse adjacency (only agent 0 connected to agent 1)
        adjacency_mask = torch.zeros(num_agents, num_agents, dtype=torch.bool)
        adjacency_mask[0, 1] = True
        adjacency_mask[1, 0] = True

        updated_states = graph_attention(agent_states, adjacency_mask)

        assert updated_states.shape == (num_agents, hidden_dim)


class TestAgentInteractionNetwork:
    """Test multi-agent interaction networks."""

    @pytest.fixture
    def agent_configs(self):
        """Create test agent configurations."""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        return [
            AgentConfig(
                agent_id=f"agent_{symbol}",
                stock_symbol=symbol,
                hidden_dim=64
            )
            for symbol in symbols
        ]

    @pytest.fixture
    def network_config(self):
        """Create test network configuration."""
        return NetworkConfig(
            num_agents=3,
            interaction_type="graph_attention",
            graph_topology="dynamic",
            communication_rounds=2
        )

    @pytest.fixture
    def mock_feature_flags(self):
        """Mock feature flags to enable agent networks."""
        with patch.object(feature_manager, 'is_enabled') as mock_is_enabled:
            mock_is_enabled.return_value = True
            yield mock_is_enabled

    def test_agent_network_initialization(self, agent_configs, network_config, mock_feature_flags):
        """Test agent network initialization."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        assert len(network.agents) == 3
        assert network.network_config == network_config
        assert hasattr(network, 'interaction_layer')
        assert hasattr(network, 'graph_constructor')
        assert hasattr(network, 'market_aggregator')

    def test_agent_network_forward_basic(self, agent_configs, network_config, mock_feature_flags):
        """Test basic agent network forward pass."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        # Create test market features
        hidden_dim = 64
        market_features = {
            "agent_AAPL": torch.randn(1, hidden_dim),
            "agent_MSFT": torch.randn(1, hidden_dim),
            "agent_GOOGL": torch.randn(1, hidden_dim)
        }

        results = network(market_features, enable_communication=False)

        assert "agent_outputs" in results
        assert "market_signal" in results
        assert "agent_states" in results
        assert len(results["agent_outputs"]) == 3

        # Check market signal shape
        market_signal = results["market_signal"]
        assert market_signal.shape == (3,)  # 3 actions: buy, hold, sell

    def test_agent_network_communication(self, agent_configs, network_config, mock_feature_flags):
        """Test agent network with communication."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        hidden_dim = 64
        market_features = {
            "agent_AAPL": torch.randn(1, hidden_dim),
            "agent_MSFT": torch.randn(1, hidden_dim)
        }

        results = network(market_features, enable_communication=True)

        assert results["num_communication_rounds"] == 2
        assert "agent_outputs" in results
        assert "market_signal" in results

    def test_dynamic_graph_construction(self, agent_configs, network_config, mock_feature_flags):
        """Test dynamic graph construction."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        num_agents = 3
        hidden_dim = 64
        agent_states = torch.randn(num_agents, hidden_dim)

        adjacency = network.construct_dynamic_graph(agent_states)

        assert adjacency.shape == (num_agents, num_agents)
        assert adjacency.dtype == torch.bool

        # Self-connections should be False
        assert not adjacency.diag().any()

    def test_market_signal_aggregation(self, agent_configs, network_config, mock_feature_flags):
        """Test market signal aggregation."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        # Mock agent outputs
        agent_outputs = {
            "agent_AAPL": (torch.tensor([0.5, 0.3, 0.2]), torch.tensor([0.8]), None),
            "agent_MSFT": (torch.tensor([0.2, 0.6, 0.2]), torch.tensor([0.6]), None),
            "agent_GOOGL": (torch.tensor([0.3, 0.3, 0.4]), torch.tensor([0.7]), None)
        }

        market_signal = network.aggregate_market_signals(agent_outputs)

        assert market_signal.shape == (3,)
        assert torch.allclose(market_signal.sum(), torch.tensor(1.0), atol=1e-6)

    def test_empty_market_features(self, agent_configs, network_config, mock_feature_flags):
        """Test network with empty market features."""
        network = AgentInteractionNetwork(network_config, agent_configs)

        results = network({})

        assert "error" in results


class TestPortfolioAgentSystem:
    """Test portfolio agent system."""

    @pytest.fixture
    def mock_feature_flags(self):
        """Mock feature flags to enable portfolio agents."""
        with patch.object(feature_manager, 'is_enabled') as mock_is_enabled:
            def side_effect(flag_name):
                return flag_name in ["enable_agent_networks", "enable_portfolio_agents"]
            mock_is_enabled.side_effect = side_effect
            yield mock_is_enabled

    def test_portfolio_system_initialization(self, mock_feature_flags):
        """Test portfolio system initialization."""
        stocks = ["AAPL", "MSFT", "GOOGL"]
        portfolio_system = PortfolioAgentSystem(stocks, hidden_dim=64)

        assert portfolio_system.stocks == stocks
        assert portfolio_system.hidden_dim == 64
        assert hasattr(portfolio_system, 'agent_network')
        assert hasattr(portfolio_system, 'portfolio_optimizer')

    def test_portfolio_system_forward(self, mock_feature_flags):
        """Test portfolio system forward pass."""
        stocks = ["AAPL", "MSFT"]
        portfolio_system = PortfolioAgentSystem(stocks, hidden_dim=64)

        stock_features = {
            "AAPL": torch.randn(1, 64),
            "MSFT": torch.randn(1, 64)
        }

        results = portfolio_system(stock_features)

        assert "portfolio_weights" in results
        assert "agent_results" in results
        assert "market_confidence" in results

        # Check portfolio weights
        weights = results["portfolio_weights"]
        assert len(weights) == 2
        assert "AAPL" in weights
        assert "MSFT" in weights

        # Weights should be non-negative and sum to 1 (approximately)
        weight_values = list(weights.values())
        assert all(w >= 0 for w in weight_values)
        assert abs(sum(weight_values) - 1.0) < 0.01


class TestFeatureFlagIntegration:
    """Test feature flag integration."""

    def test_agent_network_creation_disabled(self):
        """Test agent network creation when feature is disabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=False):
            network = create_agent_network(["AAPL", "MSFT"])
            assert network is None

    def test_agent_network_creation_enabled(self):
        """Test agent network creation when feature is enabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            network = create_agent_network(["AAPL", "MSFT"])
            assert network is not None
            assert isinstance(network, AgentInteractionNetwork)

    def test_portfolio_system_creation_disabled(self):
        """Test portfolio system creation when feature is disabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=False):
            system = create_portfolio_system(["AAPL", "MSFT"])
            assert system is None

    def test_portfolio_system_creation_enabled(self):
        """Test portfolio system creation when feature is enabled."""
        def side_effect(flag_name):
            return flag_name in ["enable_agent_networks", "enable_portfolio_agents"]

        with patch.object(feature_manager, 'is_enabled', side_effect=side_effect):
            system = create_portfolio_system(["AAPL", "MSFT"])
            assert system is not None
            assert isinstance(system, PortfolioAgentSystem)


class TestPerformanceBenchmarks:
    """Performance benchmarks for agent networks."""

    @pytest.fixture
    def large_agent_network(self):
        """Create large agent network for performance testing."""
        stocks = [f"STOCK_{i:03d}" for i in range(20)]

        with patch.object(feature_manager, 'is_enabled', return_value=True):
            return create_agent_network(stocks, hidden_dim=256, num_communication_rounds=1)

    def test_large_network_forward_pass(self, large_agent_network):
        """Test forward pass with large network."""
        if large_agent_network is None:
            pytest.skip("Agent networks not available")

        hidden_dim = 256
        market_features = {
            f"agent_STOCK_{i:03d}": torch.randn(1, hidden_dim)
            for i in range(20)
        }

        import time
        start_time = time.time()

        results = large_agent_network(market_features, enable_communication=True)

        end_time = time.time()
        processing_time = end_time - start_time

        assert "agent_outputs" in results
        assert len(results["agent_outputs"]) == 20
        assert processing_time < 5.0  # Should complete within 5 seconds

    def test_batch_processing_performance(self, large_agent_network):
        """Test batch processing performance."""
        if large_agent_network is None:
            pytest.skip("Agent networks not available")

        batch_size = 10
        hidden_dim = 256

        # Create batch of market features
        market_features = {
            f"agent_STOCK_{i:03d}": torch.randn(batch_size, hidden_dim)
            for i in range(5)  # Smaller subset for batch test
        }

        import time
        start_time = time.time()

        results = large_agent_network(market_features, enable_communication=False)

        end_time = time.time()
        processing_time = end_time - start_time

        assert processing_time < 2.0  # Batch should be faster


class TestIntegrationScenarios:
    """Integration test scenarios for agent networks."""

    def test_end_to_end_trading_scenario(self):
        """Test complete trading scenario with agent networks."""
        with patch.object(feature_manager, 'is_enabled') as mock_is_enabled:
            def side_effect(flag_name):
                return flag_name in ["enable_agent_networks", "enable_portfolio_agents"]
            mock_is_enabled.side_effect = side_effect

            # Create portfolio system
            stocks = ["AAPL", "MSFT", "GOOGL"]
            portfolio_system = create_portfolio_system(stocks, hidden_dim=128)

            if portfolio_system is None:
                pytest.skip("Portfolio agents not available")

            # Simulate market data over time
            time_steps = 5
            results = []

            for t in range(time_steps):
                # Generate realistic market features (price movements, volume, etc.)
                stock_features = {}
                for stock in stocks:
                    # Simulate OHLCV + technical indicators
                    features = torch.randn(1, 128) * (0.5 + t * 0.1)  # Increasing volatility
                    stock_features[stock] = features

                # Get portfolio allocation
                result = portfolio_system(stock_features)
                results.append(result)

                # Verify result structure
                assert "portfolio_weights" in result
                assert "market_confidence" in result

                # Check that weights are valid
                weights = result["portfolio_weights"]
                assert len(weights) == len(stocks)
                weight_sum = sum(weights.values())
                assert 0.95 <= weight_sum <= 1.05  # Allow small numerical errors

            # Verify temporal consistency
            assert len(results) == time_steps

            # Market confidence should vary over time
            confidences = [r["market_confidence"] for r in results]
            assert len(set(confidences)) > 1  # Not all the same

    def test_multi_agent_coordination_scenario(self):
        """Test multi-agent coordination in volatile market."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            stocks = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
            agent_network = create_agent_network(stocks, hidden_dim=64, num_communication_rounds=3)

            if agent_network is None:
                pytest.skip("Agent networks not available")

            # Simulate market shock scenario
            hidden_dim = 64

            # Normal market conditions
            normal_features = {
                f"agent_{stock}": torch.randn(1, hidden_dim) * 0.1
                for stock in stocks
            }

            normal_results = agent_network(normal_features, enable_communication=True)

            # Market shock conditions (high volatility)
            shock_features = {
                f"agent_{stock}": torch.randn(1, hidden_dim) * 2.0  # 20x volatility
                for stock in stocks
            }

            shock_results = agent_network(shock_features, enable_communication=True)

            # Verify different responses to different market conditions
            normal_signal = normal_results["market_signal"]
            shock_signal = shock_results["market_signal"]

            # Signals should be different under different market conditions
            assert not torch.allclose(normal_signal, shock_signal, atol=0.1)

            # Both should still be valid probability distributions
            assert torch.allclose(normal_signal.sum(), torch.tensor(1.0), atol=1e-6)
            assert torch.allclose(shock_signal.sum(), torch.tensor(1.0), atol=1e-6)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])