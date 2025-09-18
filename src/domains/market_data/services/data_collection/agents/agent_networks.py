#!/usr/bin/env python3
"""
Agent Interaction Networks for Multi-Agent Stock Modeling

Implements autonomous agent networks inspired by multi-agent systems research,
enabling emergent behavior modeling for financial markets.

Key Features:
- Multi-agent stock behavior modeling
- Graph-based agent interactions
- Emergent market dynamics
- Portfolio optimization through agent coordination
- Feature-flag controlled activation
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
import logging
import gin

from src.core.platform.config.feature_flags import require_feature, feature_gate, is_enabled

logger = logging.getLogger(__name__)


@gin.configurable
@dataclass
class AgentConfig:
    """Configuration for individual agents."""
    agent_id: str
    stock_symbol: str
    hidden_dim: int = 256
    action_dim: int = 3  # buy, hold, sell
    learning_rate: float = 0.001
    risk_tolerance: float = 0.5
    memory_horizon: int = 60
    interaction_radius: float = 0.1
    # Neural network architecture parameters
    dropout_rate: float = 0.1
    attention_heads: int = 8
    hidden_layers_ratio: int = 2  # hidden_dim // hidden_layers_ratio for intermediate layers
    metadata: Dict[str, Any] = field(default_factory=dict)


@gin.configurable
@dataclass
class NetworkConfig:
    """Configuration for agent network."""
    num_agents: int = 10
    interaction_type: str = "graph_attention"  # "graph_attention", "message_passing", "consensus"
    graph_topology: str = "dynamic"  # "dynamic", "static", "hierarchical"
    communication_rounds: int = 3
    consensus_threshold: float = 0.7
    enable_learning: bool = True
    dropout: float = 0.1
    temperature: float = 1.0


class StockAgent(nn.Module):
    """Individual stock trading agent."""

    def __init__(self, config: AgentConfig):
        super().__init__()
        self.config = config

        # Agent's internal state processing
        self.state_encoder = nn.Sequential(
            nn.Linear(config.hidden_dim, config.hidden_dim),
            nn.ReLU(),
            nn.Dropout(config.dropout_rate),
            nn.Linear(config.hidden_dim, config.hidden_dim)
        )

        # Message processing for agent communication
        self.message_encoder = nn.Linear(config.hidden_dim, config.hidden_dim)
        self.message_aggregator = nn.MultiheadAttention(
            config.hidden_dim,
            num_heads=config.attention_heads,
            dropout=config.dropout_rate,
            batch_first=True
        )

        # Action prediction
        self.action_head = nn.Sequential(
            nn.Linear(config.hidden_dim, config.hidden_dim // config.hidden_layers_ratio),
            nn.ReLU(),
            nn.Linear(config.hidden_dim // 2, config.action_dim),
            nn.Softmax(dim=-1)
        )

        # Value estimation for reinforcement learning
        self.value_head = nn.Linear(config.hidden_dim, 1)

        # Agent memory for temporal consistency
        self.register_buffer("memory", torch.zeros(config.memory_horizon, config.hidden_dim))
        self.memory_idx = 0

    def encode_state(self, market_features: torch.Tensor) -> torch.Tensor:
        """Encode current market state."""
        return self.state_encoder(market_features)

    def update_memory(self, state: torch.Tensor):
        """Update agent's memory with current state."""
        self.memory[self.memory_idx] = state.detach()
        self.memory_idx = (self.memory_idx + 1) % self.config.memory_horizon

    def get_memory_context(self) -> torch.Tensor:
        """Get agent's memory context for decision making."""
        return self.memory.mean(dim=0)

    def process_messages(self, messages: torch.Tensor, sender_masks: torch.Tensor) -> torch.Tensor:
        """Process messages from other agents."""
        if messages.size(0) == 0:
            return torch.zeros_like(self.get_memory_context())

        # Encode messages
        encoded_messages = self.message_encoder(messages)

        # Self-attention over messages
        attended_messages, _ = self.message_aggregator(
            encoded_messages.unsqueeze(0),
            encoded_messages.unsqueeze(0),
            encoded_messages.unsqueeze(0),
            key_padding_mask=~sender_masks.unsqueeze(0)
        )

        return attended_messages.squeeze(0).mean(dim=0)

    def forward(
        self,
        market_features: torch.Tensor,
        messages: Optional[torch.Tensor] = None,
        sender_masks: Optional[torch.Tensor] = None
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        Forward pass for agent decision making.

        Returns:
            actions: Action probabilities
            values: State values
            messages: Messages to send to other agents
        """
        # Encode current state
        state = self.encode_state(market_features)

        # Update memory
        self.update_memory(state)

        # Get memory context
        memory_context = self.get_memory_context()

        # Process incoming messages
        if messages is not None and sender_masks is not None:
            message_context = self.process_messages(messages, sender_masks)
        else:
            message_context = torch.zeros_like(state)

        # Combine contexts
        combined_state = state + memory_context + message_context

        # Generate actions and values
        actions = self.action_head(combined_state)
        values = self.value_head(combined_state)

        # Generate outgoing message
        outgoing_message = self.message_encoder(combined_state)

        return actions, values, outgoing_message


class GraphAttentionNetwork(nn.Module):
    """Graph attention network for agent interactions."""

    def __init__(self, hidden_dim: int, num_heads: int = 8):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.num_heads = num_heads

        self.attention = nn.MultiheadAttention(
            hidden_dim,
            num_heads,
            dropout=0.1,
            batch_first=True
        )

        self.norm = nn.LayerNorm(hidden_dim)
        self.ffn = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * 4),
            nn.ReLU(),
            nn.Dropout(config.dropout_rate),
            nn.Linear(hidden_dim * 4, hidden_dim)
        )

    def forward(self, agent_states: torch.Tensor, adjacency_mask: torch.Tensor) -> torch.Tensor:
        """
        Apply graph attention to agent states.

        Args:
            agent_states: [num_agents, hidden_dim]
            adjacency_mask: [num_agents, num_agents] - True for connected agents
        """
        # Self-attention with adjacency masking
        attended_states, attention_weights = self.attention(
            agent_states.unsqueeze(0),
            agent_states.unsqueeze(0),
            agent_states.unsqueeze(0),
            key_padding_mask=~adjacency_mask.any(dim=0).unsqueeze(0)
        )

        attended_states = attended_states.squeeze(0)

        # Residual connection and normalization
        agent_states = self.norm(agent_states + attended_states)

        # Feed-forward network
        ffn_output = self.ffn(agent_states)
        agent_states = self.norm(agent_states + ffn_output)

        return agent_states


@require_feature("enable_agent_networks")
class AgentInteractionNetwork(nn.Module):
    """Multi-agent interaction network for stock modeling."""

    def __init__(self, network_config: NetworkConfig, agent_configs: List[AgentConfig]):
        super().__init__()
        self.network_config = network_config
        self.agent_configs = agent_configs

        # Initialize agents
        self.agents = nn.ModuleDict({
            config.agent_id: StockAgent(config)
            for config in agent_configs
        })

        # Graph attention network for agent interactions
        if network_config.interaction_type == "graph_attention":
            self.interaction_layer = GraphAttentionNetwork(
                agent_configs[0].hidden_dim,
                num_heads=8
            )

        # Market graph construction
        self.graph_constructor = nn.Sequential(
            nn.Linear(agent_configs[0].hidden_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )

        # Global market state aggregator
        self.market_aggregator = nn.Sequential(
            nn.Linear(agent_configs[0].hidden_dim, 128),
            nn.ReLU(),
            nn.Linear(128, agent_configs[0].hidden_dim)
        )

        logger.info(f"AgentInteractionNetwork initialized with {len(self.agents)} agents")

    def construct_dynamic_graph(self, agent_states: torch.Tensor) -> torch.Tensor:
        """Construct dynamic interaction graph between agents."""
        num_agents = agent_states.size(0)

        # Compute pairwise similarities
        similarities = torch.zeros(num_agents, num_agents)

        for i in range(num_agents):
            for j in range(num_agents):
                if i != j:
                    # Compute interaction strength
                    combined_state = torch.cat([agent_states[i], agent_states[j]])
                    similarities[i, j] = self.graph_constructor(combined_state).item()

        # Apply interaction radius threshold
        threshold = self.network_config.consensus_threshold
        adjacency = similarities > threshold

        return adjacency

    def aggregate_market_signals(self, agent_outputs: Dict[str, Tuple]) -> torch.Tensor:
        """Aggregate signals from all agents to form market view."""
        all_actions = []
        all_values = []

        for agent_id, (actions, values, _) in agent_outputs.items():
            all_actions.append(actions)
            all_values.append(values)

        # Weighted aggregation based on agent confidence
        actions_tensor = torch.stack(all_actions)
        values_tensor = torch.stack(all_values)

        # Use values as confidence weights
        confidence_weights = F.softmax(values_tensor.squeeze(-1), dim=0)

        # Weighted market action
        market_action = (actions_tensor * confidence_weights.unsqueeze(-1)).sum(dim=0)

        return market_action

    def forward(
        self,
        market_features: Dict[str, torch.Tensor],
        enable_communication: bool = True
    ) -> Dict[str, Any]:
        """
        Forward pass for agent network.

        Args:
            market_features: {agent_id: features} for each agent
            enable_communication: Whether to enable agent communication

        Returns:
            Dictionary with agent outputs and aggregated market signals
        """
        agent_outputs = {}
        agent_states = []
        agent_messages = []

        # Initial forward pass for all agents
        for agent_id, agent in self.agents.items():
            if agent_id not in market_features:
                continue

            actions, values, message = agent(market_features[agent_id])
            agent_outputs[agent_id] = (actions, values, message)
            agent_states.append(agent(market_features[agent_id])[2])  # Get encoded state
            agent_messages.append(message)

        if not agent_states:
            return {"error": "No valid agent inputs provided"}

        agent_states_tensor = torch.stack(agent_states)

        # Agent communication rounds
        if enable_communication and self.network_config.communication_rounds > 0:
            for round_idx in range(self.network_config.communication_rounds):
                # Construct dynamic graph
                if self.network_config.graph_topology == "dynamic":
                    adjacency = self.construct_dynamic_graph(agent_states_tensor)
                else:
                    # Static fully connected graph
                    num_agents = len(agent_states)
                    adjacency = torch.ones(num_agents, num_agents) - torch.eye(num_agents)

                # Apply graph attention
                if self.network_config.interaction_type == "graph_attention":
                    agent_states_tensor = self.interaction_layer(agent_states_tensor, adjacency)

                # Update agent states based on interactions
                for idx, (agent_id, agent) in enumerate(self.agents.items()):
                    if agent_id not in market_features:
                        continue

                    # Get messages from connected agents
                    connected_agents = adjacency[idx].bool()
                    if connected_agents.any():
                        messages = torch.stack([agent_messages[i] for i in range(len(agent_messages)) if connected_agents[i]])
                        sender_masks = torch.ones(messages.size(0), dtype=torch.bool)

                        # Re-run agent with messages
                        actions, values, message = agent(
                            market_features[agent_id],
                            messages,
                            sender_masks
                        )
                        agent_outputs[agent_id] = (actions, values, message)
                        agent_messages[idx] = message

        # Aggregate market signals
        market_signal = self.aggregate_market_signals(agent_outputs)

        return {
            "agent_outputs": agent_outputs,
            "market_signal": market_signal,
            "agent_states": agent_states_tensor,
            "num_communication_rounds": self.network_config.communication_rounds if enable_communication else 0
        }


@require_feature("enable_portfolio_agents")
class PortfolioAgentSystem(nn.Module):
    """Multi-agent portfolio optimization system."""

    def __init__(self, stocks: List[str], hidden_dim: int = 256):
        super().__init__()
        self.stocks = stocks
        self.hidden_dim = hidden_dim

        # Create agent configs for each stock
        agent_configs = [
            AgentConfig(
                agent_id=f"agent_{symbol}",
                stock_symbol=symbol,
                hidden_dim=hidden_dim
            )
            for symbol in stocks
        ]

        # Initialize agent network
        network_config = NetworkConfig(
            num_agents=len(stocks),
            interaction_type="graph_attention",
            graph_topology="dynamic",
            communication_rounds=2
        )

        self.agent_network = AgentInteractionNetwork(network_config, agent_configs)

        # Portfolio optimization layer
        self.portfolio_optimizer = nn.Sequential(
            nn.Linear(hidden_dim, 128),
            nn.ReLU(),
            nn.Linear(128, len(stocks)),
            nn.Softmax(dim=-1)  # Portfolio weights
        )

        logger.info(f"PortfolioAgentSystem initialized for {len(stocks)} stocks")

    def forward(self, stock_features: Dict[str, torch.Tensor]) -> Dict[str, Any]:
        """
        Generate portfolio allocation using multi-agent system.

        Args:
            stock_features: {stock_symbol: features} for each stock

        Returns:
            Portfolio weights and agent analysis
        """
        # Convert stock features to agent features
        agent_features = {}
        for stock in self.stocks:
            if stock in stock_features:
                agent_features[f"agent_{stock}"] = stock_features[stock]

        # Run agent network
        agent_results = self.agent_network(agent_features)

        if "error" in agent_results:
            return agent_results

        # Generate portfolio weights from market signal
        market_signal = agent_results["market_signal"]
        portfolio_weights = self.portfolio_optimizer(market_signal)

        return {
            "portfolio_weights": dict(zip(self.stocks, portfolio_weights.tolist())),
            "agent_results": agent_results,
            "market_confidence": market_signal.max().item()
        }


# Feature-gated factory functions
@feature_gate("enable_agent_networks")
def create_agent_network(
    stocks: List[str],
    hidden_dim: int = 256,
    num_communication_rounds: int = 2
) -> Optional[AgentInteractionNetwork]:
    """Factory function to create agent interaction network."""
    if not is_enabled("enable_agent_networks"):
        logger.warning("Agent networks feature is disabled")
        return None

    agent_configs = [
        AgentConfig(
            agent_id=f"agent_{symbol}",
            stock_symbol=symbol,
            hidden_dim=hidden_dim
        )
        for symbol in stocks
    ]

    network_config = NetworkConfig(
        num_agents=len(stocks),
        interaction_type="graph_attention",
        communication_rounds=num_communication_rounds
    )

    return AgentInteractionNetwork(network_config, agent_configs)


@feature_gate("enable_portfolio_agents")
def create_portfolio_system(
    stocks: List[str],
    hidden_dim: int = 256
) -> Optional[PortfolioAgentSystem]:
    """Factory function to create portfolio agent system."""
    if not is_enabled("enable_portfolio_agents"):
        logger.warning("Portfolio agents feature is disabled")
        return None

    return PortfolioAgentSystem(stocks, hidden_dim)