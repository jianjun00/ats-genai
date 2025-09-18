#!/usr/bin/env python3
"""
Optimal Loss Functions for Autonomous Driving Inspired Financial Transformer

Based on comprehensive research synthesis from autonomous driving and financial trading domains.
Implements state-of-the-art multi-task learning, risk-aware, and uncertainty-weighted loss functions.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class FinancialAVLoss(nn.Module):
    """
    Unified loss function combining insights from autonomous driving and financial trading.

    Key Features:
    - Uncertainty-weighted multi-task learning (from AV research)
    - Risk-aware penalties (CVaR, drawdown from finance research)
    - Focal loss enhancement for difficult predictions
    - Temporal consistency requirements
    - Safety-first design principles
    """

    def __init__(
        self,
        num_tasks: int = 5,
        alpha_cvar: float = 0.05,
        lambda_drawdown: float = 2.0,
        gamma_focal: float = 2.0,
        temporal_weight: float = 0.1,
        safety_weight: float = 1.5
    ):
        super().__init__()

        # Learnable task uncertainties (from AV uncertainty weighting)
        self.log_vars = nn.Parameter(torch.zeros(num_tasks))

        # Risk parameters (from finance research)
        self.alpha_cvar = alpha_cvar
        self.lambda_drawdown = lambda_drawdown
        self.gamma_focal = gamma_focal
        self.temporal_weight = temporal_weight
        self.safety_weight = safety_weight

        # Task names for interpretability
        self.task_names = ['price_movement', 'volatility', 'volume_profile', 'regime_change', 'risk_assessment']

        logger.info(f"Initialized FinancialAVLoss with {num_tasks} tasks")
        logger.info(f"Risk parameters: CVaR α={alpha_cvar}, Drawdown λ={lambda_drawdown}")

    def forward(
        self,
        predictions: Dict[str, torch.Tensor],
        targets: Dict[str, torch.Tensor],
        historical_predictions: Optional[Dict[str, torch.Tensor]] = None,
        market_context: Optional[Dict[str, torch.Tensor]] = None
    ) -> Dict[str, torch.Tensor]:
        """
        Forward pass computing unified loss.

        Args:
            predictions: Model predictions for all tasks
            targets: Ground truth targets
            historical_predictions: Previous predictions for temporal consistency
            market_context: Additional market context for adaptive weighting

        Returns:
            Dictionary containing total loss and detailed loss components
        """

        device = next(iter(predictions.values())).device
        batch_size = next(iter(predictions.values())).shape[0]

        total_loss = torch.tensor(0.0, device=device)
        loss_components = {}

        # =====================================================================
        # MULTI-TASK LOSSES WITH UNCERTAINTY WEIGHTING
        # =====================================================================

        task_losses = {}

        # Task 1: Price Movement Prediction (Primary task)
        if 'price_movement' in predictions and 'price_movement' in targets:
            price_loss = self._compute_enhanced_mse_loss(
                predictions['price_movement'],
                targets['price_movement'],
                apply_focal=True
            )
            task_losses['price_movement'] = price_loss

        # Task 2: Volatility Prediction
        if 'volatility' in predictions and 'volatility' in targets:
            vol_loss = F.mse_loss(predictions['volatility'], targets['volatility'])
            task_losses['volatility'] = vol_loss

        # Task 3: Volume Profile Prediction
        if 'volume_profile' in predictions and 'volume_profile' in targets:
            volume_loss = F.mse_loss(predictions['volume_profile'], targets['volume_profile'])
            task_losses['volume_profile'] = volume_loss

        # Task 4: Market Regime Classification (from AV object detection)
        if 'regime_change' in predictions and 'regime_change' in targets:
            regime_loss = self._compute_focal_cross_entropy(
                predictions['regime_change'],
                targets['regime_change']
            )
            task_losses['regime_change'] = regime_loss

        # Task 5: Risk Assessment (Critical like collision detection)
        if 'risk_assessment' in predictions and 'risk_assessment' in targets:
            risk_loss = self._compute_enhanced_mse_loss(
                predictions['risk_assessment'],
                targets['risk_assessment'],
                apply_focal=True
            )
            task_losses['risk_assessment'] = risk_loss

        # Apply uncertainty weighting to multi-task losses
        uncertainty_weighted_loss = torch.tensor(0.0, device=device)
        task_uncertainties = {}

        for i, task_name in enumerate(self.task_names[:len(task_losses)]):
            if task_name in task_losses:
                precision = torch.exp(-self.log_vars[i])
                weighted_loss = precision * task_losses[task_name] + self.log_vars[i]
                uncertainty_weighted_loss += weighted_loss
                task_uncertainties[task_name] = torch.exp(self.log_vars[i]).item()

        total_loss += uncertainty_weighted_loss
        loss_components['uncertainty_weighted_loss'] = uncertainty_weighted_loss

        # =====================================================================
        # FINANCIAL RISK PENALTIES (Safety-Critical Design)
        # =====================================================================

        risk_penalties = torch.tensor(0.0, device=device)

        if 'price_movement' in predictions:
            returns = predictions['price_movement']

            # CVaR penalty for tail risk control
            cvar_loss = self._compute_cvar_loss(returns)

            # Maximum drawdown penalty
            drawdown_loss = self._compute_drawdown_loss(returns)

            # Combined risk penalty
            risk_penalties = self.safety_weight * (
                self.lambda_drawdown * drawdown_loss + cvar_loss
            )

            total_loss += risk_penalties
            loss_components['cvar_loss'] = cvar_loss
            loss_components['drawdown_loss'] = drawdown_loss

        loss_components['risk_penalties'] = risk_penalties

        # =====================================================================
        # TEMPORAL CONSISTENCY PENALTY
        # =====================================================================

        temporal_loss = torch.tensor(0.0, device=device)

        if historical_predictions is not None:
            temporal_loss = self._compute_temporal_consistency_loss(
                predictions, historical_predictions
            )
            total_loss += self.temporal_weight * temporal_loss

        loss_components['temporal_loss'] = temporal_loss

        # =====================================================================
        # ADAPTIVE WEIGHTING BASED ON MARKET CONTEXT
        # =====================================================================

        if market_context is not None:
            market_adaptive_loss = self._compute_market_adaptive_penalty(
                predictions, targets, market_context
            )
            total_loss += market_adaptive_loss
            loss_components['market_adaptive_loss'] = market_adaptive_loss

        # =====================================================================
        # FINAL LOSS COMBINATION
        # =====================================================================

        loss_components['total_loss'] = total_loss
        loss_components['task_losses'] = task_losses
        loss_components['task_uncertainties'] = task_uncertainties

        return loss_components

    def _compute_enhanced_mse_loss(
        self,
        predictions: torch.Tensor,
        targets: torch.Tensor,
        apply_focal: bool = False
    ) -> torch.Tensor:
        """Enhanced MSE loss with optional focal weighting."""
        base_loss = F.mse_loss(predictions, targets, reduction='none')

        if apply_focal:
            # Apply focal loss weighting to emphasize difficult predictions
            prediction_error = torch.abs(predictions - targets)
            focal_weights = torch.pow(prediction_error, self.gamma_focal)
            focal_loss = torch.mean(focal_weights * base_loss)
            return focal_loss

        return torch.mean(base_loss)

    def _compute_focal_cross_entropy(
        self,
        predictions: torch.Tensor,
        targets: torch.Tensor
    ) -> torch.Tensor:
        """Focal loss for classification tasks."""
        ce_loss = F.cross_entropy(predictions, targets, reduction='none')
        pt = torch.exp(-ce_loss)
        focal_loss = (1 - pt) ** self.gamma_focal * ce_loss
        return torch.mean(focal_loss)

    def _compute_cvar_loss(
        self,
        returns: torch.Tensor,
        alpha: Optional[float] = None
    ) -> torch.Tensor:
        """Compute Conditional Value at Risk (CVaR) loss for tail risk control."""
        if alpha is None:
            alpha = self.alpha_cvar

        # Flatten returns across batch and sequence dimensions
        flat_returns = returns.view(-1)

        # Calculate VaR threshold
        var_threshold = torch.quantile(flat_returns, alpha)

        # Calculate CVaR (expected loss beyond VaR)
        tail_losses = flat_returns[flat_returns <= var_threshold]

        if len(tail_losses) > 0:
            cvar = -torch.mean(tail_losses)
        else:
            # If no tail losses, use VaR as approximation
            cvar = -var_threshold

        return cvar

    def _compute_drawdown_loss(self, returns: torch.Tensor) -> torch.Tensor:
        """Compute maximum drawdown penalty."""
        # Compute cumulative returns
        if returns.dim() == 3:  # [batch, seq, 1]
            cumulative_returns = torch.cumsum(returns.squeeze(-1), dim=1)
        else:  # [batch, seq]
            cumulative_returns = torch.cumsum(returns, dim=1)

        # Calculate running maximum
        running_max = torch.cummax(cumulative_returns, dim=1)[0]

        # Calculate drawdown
        drawdown = cumulative_returns - running_max

        # Maximum drawdown per sample
        max_drawdown = -torch.min(drawdown, dim=1)[0]

        # Return mean maximum drawdown
        return torch.mean(max_drawdown)

    def _compute_temporal_consistency_loss(
        self,
        predictions: Dict[str, torch.Tensor],
        historical_predictions: Dict[str, torch.Tensor]
    ) -> torch.Tensor:
        """Compute temporal consistency penalty."""
        consistency_loss = torch.tensor(0.0, device=predictions['price_movement'].device)

        for task_name in predictions:
            if task_name in historical_predictions:
                current_pred = predictions[task_name]
                historical_pred = historical_predictions[task_name]

                # L2 distance between consecutive predictions
                consistency_penalty = F.mse_loss(current_pred, historical_pred)
                consistency_loss += consistency_penalty

        return consistency_loss

    def _compute_market_adaptive_penalty(
        self,
        predictions: Dict[str, torch.Tensor],
        targets: Dict[str, torch.Tensor],
        market_context: Dict[str, torch.Tensor]
    ) -> torch.Tensor:
        """Adaptive penalty based on market conditions."""
        adaptive_loss = torch.tensor(0.0, device=predictions['price_movement'].device)

        if 'volatility_regime' in market_context:
            # Increase penalty during high volatility periods
            vol_regime = market_context['volatility_regime']
            high_vol_mask = vol_regime > 0.5  # Assuming normalized volatility

            if 'price_movement' in predictions and high_vol_mask.any():
                price_error = F.mse_loss(
                    predictions['price_movement'][high_vol_mask],
                    targets['price_movement'][high_vol_mask],
                    reduction='mean'
                )
                adaptive_loss += 2.0 * price_error  # Higher penalty in volatile periods

        return adaptive_loss


class FinancialMetricsCalculator:
    """
    Comprehensive evaluation metrics calculator for financial trading models.
    Implements state-of-the-art metrics from both finance and autonomous driving research.
    """

    @staticmethod
    def compute_comprehensive_metrics(
        predictions: Dict[str, torch.Tensor],
        targets: Dict[str, torch.Tensor],
        returns_series: Optional[torch.Tensor] = None
    ) -> Dict[str, float]:
        """
        Compute comprehensive financial performance metrics.

        Args:
            predictions: Model predictions
            targets: Ground truth targets
            returns_series: Time series of returns for temporal metrics

        Returns:
            Dictionary of computed metrics
        """

        metrics = {}

        # Convert to numpy for calculations
        if 'price_movement' in predictions:
            pred_returns = predictions['price_movement'].detach().cpu().numpy().flatten()
            true_returns = targets['price_movement'].detach().cpu().numpy().flatten()

            # ===================================================================
            # DIRECTIONAL ACCURACY METRICS
            # ===================================================================

            # Basic directional accuracy
            pred_direction = np.sign(pred_returns)
            true_direction = np.sign(true_returns)
            directional_accuracy = np.mean(pred_direction == true_direction)
            metrics['directional_accuracy'] = float(directional_accuracy)

            # Confidence-weighted directional accuracy (if confidence available)
            if 'confidence' in predictions:
                confidence = predictions['confidence'].detach().cpu().numpy().flatten()
                high_conf_mask = confidence > np.percentile(confidence, 70)
                if np.sum(high_conf_mask) > 0:
                    high_conf_accuracy = np.mean(
                        pred_direction[high_conf_mask] == true_direction[high_conf_mask]
                    )
                    metrics['high_confidence_accuracy'] = float(high_conf_accuracy)

            # ===================================================================
            # RISK-ADJUSTED RETURN METRICS
            # ===================================================================

            # Sharpe Ratio (annualized)
            if np.std(pred_returns) > 1e-8:
                sharpe_ratio = np.mean(pred_returns) / np.std(pred_returns) * np.sqrt(252)
                metrics['sharpe_ratio'] = float(sharpe_ratio)

            # Sortino Ratio (downside deviation only)
            downside_returns = pred_returns[pred_returns < 0]
            if len(downside_returns) > 0:
                sortino_ratio = np.mean(pred_returns) / np.std(downside_returns) * np.sqrt(252)
                metrics['sortino_ratio'] = float(sortino_ratio)

            # Information Ratio
            excess_returns = pred_returns - true_returns
            if np.std(excess_returns) > 1e-8:
                information_ratio = np.mean(excess_returns) / np.std(excess_returns)
                metrics['information_ratio'] = float(information_ratio)

            # ===================================================================
            # RISK CONTROL METRICS
            # ===================================================================

            # Maximum Drawdown
            cumulative_returns = np.cumsum(pred_returns)
            running_max = np.maximum.accumulate(cumulative_returns)
            drawdown = cumulative_returns - running_max
            max_drawdown = np.min(drawdown)
            metrics['max_drawdown'] = float(max_drawdown)
            metrics['max_drawdown_pct'] = float(abs(max_drawdown) * 100)

            # Value at Risk (VaR) at 95% confidence
            var_95 = np.percentile(pred_returns, 5)
            metrics['var_95'] = float(var_95)

            # Conditional Value at Risk (CVaR)
            tail_losses = pred_returns[pred_returns <= var_95]
            if len(tail_losses) > 0:
                cvar_95 = np.mean(tail_losses)
                metrics['cvar_95'] = float(cvar_95)

            # ===================================================================
            # CORRELATION AND PREDICTIVE POWER
            # ===================================================================

            # Correlation with actual returns
            if len(pred_returns) > 1:
                correlation = np.corrcoef(pred_returns, true_returns)[0, 1]
                if not np.isnan(correlation):
                    metrics['correlation'] = float(correlation)

            # Hit Rate (percentage of correct direction predictions)
            hit_rate = directional_accuracy
            metrics['hit_rate'] = float(hit_rate * 100)

            # ===================================================================
            # TRADING PERFORMANCE METRICS
            # ===================================================================

            # Profit Factor (if we have position-based returns)
            positive_returns = pred_returns[pred_returns > 0]
            negative_returns = pred_returns[pred_returns < 0]

            if len(positive_returns) > 0 and len(negative_returns) > 0:
                profit_factor = np.sum(positive_returns) / abs(np.sum(negative_returns))
                metrics['profit_factor'] = float(profit_factor)

            # Win Rate
            win_rate = len(positive_returns) / len(pred_returns) if len(pred_returns) > 0 else 0
            metrics['win_rate'] = float(win_rate * 100)

            # Average Win vs Average Loss
            if len(positive_returns) > 0:
                avg_win = np.mean(positive_returns)
                metrics['avg_win'] = float(avg_win)

            if len(negative_returns) > 0:
                avg_loss = np.mean(negative_returns)
                metrics['avg_loss'] = float(avg_loss)

            # ===================================================================
            # ROBUSTNESS METRICS
            # ===================================================================

            # Prediction volatility (stability measure)
            pred_volatility = np.std(pred_returns)
            metrics['prediction_volatility'] = float(pred_volatility)

            # Prediction error metrics
            mse = np.mean((pred_returns - true_returns) ** 2)
            mae = np.mean(np.abs(pred_returns - true_returns))
            metrics['mse'] = float(mse)
            metrics['mae'] = float(mae)

            # R-squared
            ss_res = np.sum((true_returns - pred_returns) ** 2)
            ss_tot = np.sum((true_returns - np.mean(true_returns)) ** 2)
            if ss_tot > 1e-8:
                r_squared = 1 - (ss_res / ss_tot)
                metrics['r_squared'] = float(r_squared)

        return metrics

    @staticmethod
    def compute_regime_aware_metrics(
        predictions: Dict[str, torch.Tensor],
        targets: Dict[str, torch.Tensor],
        regime_labels: torch.Tensor
    ) -> Dict[str, Dict[str, float]]:
        """
        Compute metrics broken down by market regime.

        Args:
            predictions: Model predictions
            targets: Ground truth targets
            regime_labels: Market regime classification

        Returns:
            Nested dictionary with metrics per regime
        """

        regime_metrics = {}
        unique_regimes = torch.unique(regime_labels)

        for regime in unique_regimes:
            regime_mask = regime_labels == regime

            if regime_mask.sum() > 0:
                # Filter predictions and targets for this regime
                regime_predictions = {
                    key: val[regime_mask] for key, val in predictions.items()
                }
                regime_targets = {
                    key: val[regime_mask] for key, val in targets.items()
                }

                # Compute metrics for this regime
                regime_metrics[f'regime_{int(regime)}'] = (
                    FinancialMetricsCalculator.compute_comprehensive_metrics(
                        regime_predictions, regime_targets
                    )
                )

        return regime_metrics


class LossScheduler:
    """
    Dynamic loss component scheduling for optimal training progression.
    Inspired by curriculum learning approaches from autonomous driving research.
    """

    def __init__(self, total_epochs: int):
        self.total_epochs = total_epochs
        self.current_epoch = 0

    def get_loss_weights(self, epoch: int) -> Dict[str, float]:
        """
        Get dynamic loss weights based on training progress.

        Args:
            epoch: Current training epoch

        Returns:
            Dictionary of loss component weights
        """
        self.current_epoch = epoch
        progress = epoch / self.total_epochs

        weights = {}

        # Start with basic prediction, gradually add risk penalties
        if progress < 0.3:
            # Early training: focus on basic prediction accuracy
            weights['task_weight'] = 1.0
            weights['risk_weight'] = 0.1
            weights['temporal_weight'] = 0.05
        elif progress < 0.7:
            # Mid training: introduce risk awareness
            weights['task_weight'] = 0.8
            weights['risk_weight'] = 0.5
            weights['temporal_weight'] = 0.1
        else:
            # Late training: full multi-objective optimization
            weights['task_weight'] = 0.7
            weights['risk_weight'] = 1.0
            weights['temporal_weight'] = 0.3

        return weights


# Example usage and testing
if __name__ == "__main__":
    # Test the loss function with dummy data
    device = torch.device('cpu')
    batch_size, seq_len = 4, 10

    # Create dummy predictions and targets
    predictions = {
        'price_movement': torch.randn(batch_size, seq_len, 1),
        'volatility': torch.abs(torch.randn(batch_size, seq_len, 1)),
        'volume_profile': torch.abs(torch.randn(batch_size, seq_len, 1)),
        'regime_change': torch.randn(batch_size, seq_len, 4),  # 4 classes
        'risk_assessment': torch.abs(torch.randn(batch_size, seq_len, 1))
    }

    targets = {
        'price_movement': torch.randn(batch_size, seq_len, 1),
        'volatility': torch.abs(torch.randn(batch_size, seq_len, 1)),
        'volume_profile': torch.abs(torch.randn(batch_size, seq_len, 1)),
        'regime_change': torch.randint(0, 4, (batch_size, seq_len)),
        'risk_assessment': torch.abs(torch.randn(batch_size, seq_len, 1))
    }

    # Initialize loss function
    loss_fn = FinancialAVLoss(num_tasks=5)

    # Compute loss
    loss_components = loss_fn(predictions, targets)

    print("Loss Components:")
    for key, value in loss_components.items():
        if isinstance(value, torch.Tensor):
            print(f"  {key}: {value.item():.6f}")
        else:
            print(f"  {key}: {value}")

    # Test metrics calculator
    metrics = FinancialMetricsCalculator.compute_comprehensive_metrics(
        predictions, targets
    )

    print("\nComputed Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value:.4f}")