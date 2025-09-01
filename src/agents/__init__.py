#!/usr/bin/env python3
"""
Agent-Based Systems for Financial Modeling

Multi-agent systems for stock trading, portfolio optimization, and market modeling.
Inspired by autonomous driving and swarm intelligence research.

Key Components:
- AgentInteractionNetwork: Multi-agent stock behavior modeling
- PortfolioAgentSystem: Agent-based portfolio optimization
- StockAgent: Individual trading agents
- GraphAttentionNetwork: Agent interaction networks

All components are feature-flag controlled for safe deployment.
"""

from config.feature_flags import is_enabled

# Conditionally import based on feature flags
if is_enabled("enable_agent_networks"):
    from .agent_networks import (
        create_agent_network
    )
    
    __all__ = [
        'AgentInteractionNetwork',
        'StockAgent', 
        'GraphAttentionNetwork',
        'AgentConfig',
        'NetworkConfig',
        'create_agent_network'
    ]
    
    if is_enabled("enable_portfolio_agents"):
        from .agent_networks import (
            create_portfolio_system
        )
        __all__.extend([
            'PortfolioAgentSystem',
            'create_portfolio_system'
        ])

else:
    __all__ = []
    
    # Provide stubs when features are disabled
    def create_agent_network(*args, **kwargs):
        return None
    
    def create_portfolio_system(*args, **kwargs):
        return None