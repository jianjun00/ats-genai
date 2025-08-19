import React, { useState, useEffect } from 'react';
import { 
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, 
  Tooltip, ResponsiveContainer, Legend, ReferenceLine, ScatterChart, Scatter
} from 'recharts';
import './ModelComparisonDashboard.css';

const ModelComparisonDashboard = () => {
  const [availableConfigs, setAvailableConfigs] = useState([]);
  const [baselineConfig, setBaselineConfig] = useState('');
  const [testConfig, setTestConfig] = useState('');
  const [comparisonResults, setComparisonResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';

  // Mock model configurations for demonstration
  useEffect(() => {
    setAvailableConfigs([
      {
        id: 'sr_baseline',
        name: 'Support/Resistance Baseline',
        description: 'Conservative S/R model with standard parameters',
        type: 'support_resistance',
        parameters: {
          learning_rate: 0.001,
          hidden_dims: [256, 128, 64],
          epochs: 100,
          dropout: 0.3
        }
      },
      {
        id: 'sr_enhanced',
        name: 'Enhanced S/R Model',
        description: 'Enhanced S/R with deeper architecture and advanced features',
        type: 'support_resistance',
        parameters: {
          learning_rate: 0.0005,
          hidden_dims: [512, 256, 128, 64],
          epochs: 150,
          dropout: 0.4
        }
      },
      {
        id: 'adaptive_weekly',
        name: 'Adaptive Weekly Retraining',
        description: 'Weekly retraining adaptive model',
        type: 'adaptive',
        parameters: {
          retrain_frequency: 7,
          learning_rate: 0.001,
          rolling_window: 365,
          learning_rate_decay: 0.95
        }
      },
      {
        id: 'adaptive_daily',
        name: 'Adaptive Daily Retraining',
        description: 'Daily retraining adaptive model for rapid changes',
        type: 'adaptive',
        parameters: {
          retrain_frequency: 1,
          learning_rate: 0.0008,
          rolling_window: 180,
          learning_rate_decay: 0.98
        }
      }
    ]);
  }, []);

  const runModelComparison = async () => {
    if (!baselineConfig || !testConfig) {
      setError('Please select both baseline and test configurations');
      return;
    }

    if (baselineConfig === testConfig) {
      setError('Baseline and test configurations must be different');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Simulate model comparison results (in real implementation, this would call the API)
      await new Promise(resolve => setTimeout(resolve, 2000)); // Simulate processing time

      const mockResults = generateMockComparisonResults(baselineConfig, testConfig);
      setComparisonResults(mockResults);

    } catch (err) {
      setError(`Comparison failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const generateMockComparisonResults = (baseline, test) => {
    const baselineConfig = availableConfigs.find(c => c.id === baseline);
    const testConfig = availableConfigs.find(c => c.id === test);

    // Generate realistic performance metrics
    const baselineMetrics = {
      total_return: 0.125 + Math.random() * 0.05,
      sharpe_ratio: 1.2 + Math.random() * 0.3,
      max_drawdown: -(0.08 + Math.random() * 0.04),
      win_rate: 0.58 + Math.random() * 0.1,
      volatility: 0.16 + Math.random() * 0.04,
      profit_factor: 1.3 + Math.random() * 0.4,
      num_trades: 120 + Math.floor(Math.random() * 60)
    };

    // Test model performs slightly better on average
    const performanceBoost = test.includes('enhanced') ? 0.25 : 
                           test.includes('adaptive') ? 0.15 : 0.1;

    const testMetrics = {
      total_return: baselineMetrics.total_return * (1 + performanceBoost + (Math.random() - 0.5) * 0.1),
      sharpe_ratio: baselineMetrics.sharpe_ratio * (1 + performanceBoost * 0.5 + (Math.random() - 0.5) * 0.2),
      max_drawdown: baselineMetrics.max_drawdown * (1 - performanceBoost * 0.3 + (Math.random() - 0.5) * 0.2),
      win_rate: Math.min(0.85, baselineMetrics.win_rate * (1 + performanceBoost * 0.2 + (Math.random() - 0.5) * 0.1)),
      volatility: baselineMetrics.volatility * (1 + (Math.random() - 0.5) * 0.2),
      profit_factor: baselineMetrics.profit_factor * (1 + performanceBoost * 0.3 + (Math.random() - 0.5) * 0.2),
      num_trades: baselineMetrics.num_trades + Math.floor((Math.random() - 0.5) * 40)
    };

    // Calculate statistical significance
    const returnDifference = testMetrics.total_return - baselineMetrics.total_return;
    const pooledStd = Math.sqrt((Math.pow(0.02, 2) + Math.pow(0.025, 2)) / 2); // Mock standard deviations
    const tStat = Math.abs(returnDifference) / (pooledStd * Math.sqrt(2/120)); // Assuming ~120 observations
    const pValue = tStat > 2.5 ? 0.003 : tStat > 1.96 ? 0.05 : 0.15;
    const cohensD = returnDifference / pooledStd;

    // Generate recommendation
    const isSignificant = pValue < 0.05;
    const performanceImprovement = returnDifference > 0;
    const largeEffect = Math.abs(cohensD) > 0.8;

    let recommendation, confidence, reasons, concerns, nextSteps;

    if (isSignificant && performanceImprovement && largeEffect) {
      recommendation = 'adopt_test';
      confidence = 'high';
      reasons = [
        `${((testMetrics.total_return / baselineMetrics.total_return - 1) * 100).toFixed(1)}% better returns`,
        'Statistically significant performance difference',
        `Large effect size (Cohen's d = ${cohensD.toFixed(2)})`,
        'Superior risk-adjusted returns'
      ];
      concerns = [
        'Higher model complexity may require more resources',
        'Need to validate on out-of-sample data'
      ];
      nextSteps = [
        'Deploy to staging environment for validation',
        'Monitor performance over 30-day period',
        'Conduct additional out-of-sample testing'
      ];
    } else if (performanceImprovement && isSignificant) {
      recommendation = 'adopt_test';
      confidence = 'medium';
      reasons = [
        'Statistically significant improvement',
        'Better risk-adjusted performance'
      ];
      concerns = [
        'Effect size is moderate',
        'May not justify increased complexity'
      ];
      nextSteps = [
        'Extended backtesting period',
        'Cost-benefit analysis of implementation'
      ];
    } else if (performanceImprovement) {
      recommendation = 'requires_further_testing';
      confidence = 'low';
      reasons = [
        'Some performance improvement observed'
      ];
      concerns = [
        'Improvement not statistically significant',
        'May be due to random variation'
      ];
      nextSteps = [
        'Extend testing period',
        'Increase sample size',
        'Test on different market conditions'
      ];
    } else {
      recommendation = 'keep_baseline';
      confidence = 'high';
      reasons = [
        'Baseline model performs as well or better',
        'Lower complexity preferred'
      ];
      concerns = [];
      nextSteps = [
        'Continue monitoring baseline performance',
        'Investigate other enhancement opportunities'
      ];
    }

    // Generate daily performance comparison data
    const performanceComparison = [];
    const tradingDays = 180;
    let baselineCumReturn = 0;
    let testCumReturn = 0;

    for (let i = 0; i < tradingDays; i++) {
      const baselineDailyReturn = (Math.random() - 0.5) * 0.03 + (baselineMetrics.total_return / tradingDays);
      const testDailyReturn = (Math.random() - 0.5) * 0.03 + (testMetrics.total_return / tradingDays);

      baselineCumReturn = (1 + baselineCumReturn) * (1 + baselineDailyReturn) - 1;
      testCumReturn = (1 + testCumReturn) * (1 + testDailyReturn) - 1;

      if (i % 5 === 0) { // Show every 5th day to reduce data points
        performanceComparison.push({
          day: i + 1,
          baseline: baselineCumReturn,
          test: testCumReturn,
          difference: testCumReturn - baselineCumReturn
        });
      }
    }

    return {
      baseline_config: baselineConfig,
      test_config: testConfig,
      baseline_metrics: baselineMetrics,
      test_metrics: testMetrics,
      statistical_analysis: {
        p_value: pValue,
        cohens_d: cohensD,
        confidence_interval: [returnDifference - 1.96 * pooledStd, returnDifference + 1.96 * pooledStd],
        is_significant: isSignificant
      },
      recommendation: {
        decision: recommendation,
        confidence: confidence,
        reasons: reasons,
        concerns: concerns,
        next_steps: nextSteps
      },
      performance_comparison: performanceComparison,
      comparison_date: new Date().toISOString()
    };
  };

  const formatPercentage = (value, decimals = 2) => {
    return `${(value * 100).toFixed(decimals)}%`;
  };

  const formatDecimal = (value, decimals = 3) => {
    return value.toFixed(decimals);
  };

  const getRecommendationColor = (decision) => {
    switch (decision) {
      case 'adopt_test': return '#00ff88';
      case 'keep_baseline': return '#00d4ff';
      case 'requires_further_testing': return '#ffa502';
      default: return '#ffffff';
    }
  };

  const getRecommendationIcon = (decision) => {
    switch (decision) {
      case 'adopt_test': return '🚀';
      case 'keep_baseline': return '🛡️';
      case 'requires_further_testing': return '🔬';
      default: return '❓';
    }
  };

  const getConfidenceColor = (confidence) => {
    switch (confidence) {
      case 'high': return '#00ff88';
      case 'medium': return '#ffa502';
      case 'low': return '#ff4757';
      default: return '#ffffff';
    }
  };

  return (
    <div className="model-comparison-dashboard">
      <div className="comparison-header">
        <h2>🔬 Model Configuration Comparison</h2>
        <p>Compare different model configurations with statistical significance testing</p>
      </div>

      {/* Configuration Selection */}
      <div className="config-selection">
        <div className="config-row">
          <div className="config-selector">
            <label>Baseline Configuration</label>
            <select 
              value={baselineConfig} 
              onChange={(e) => setBaselineConfig(e.target.value)}
            >
              <option value="">Select baseline configuration...</option>
              {availableConfigs.map(config => (
                <option key={config.id} value={config.id}>
                  {config.name}
                </option>
              ))}
            </select>
            {baselineConfig && (
              <div className="config-details">
                <span className="config-type">{availableConfigs.find(c => c.id === baselineConfig)?.type}</span>
                <span className="config-desc">{availableConfigs.find(c => c.id === baselineConfig)?.description}</span>
              </div>
            )}
          </div>

          <div className="vs-divider">VS</div>

          <div className="config-selector">
            <label>Test Configuration</label>
            <select 
              value={testConfig} 
              onChange={(e) => setTestConfig(e.target.value)}
            >
              <option value="">Select test configuration...</option>
              {availableConfigs.map(config => (
                <option key={config.id} value={config.id}>
                  {config.name}
                </option>
              ))}
            </select>
            {testConfig && (
              <div className="config-details">
                <span className="config-type">{availableConfigs.find(c => c.id === testConfig)?.type}</span>
                <span className="config-desc">{availableConfigs.find(c => c.id === testConfig)?.description}</span>
              </div>
            )}
          </div>
        </div>

        <button 
          className="run-comparison-btn"
          onClick={runModelComparison}
          disabled={loading || !baselineConfig || !testConfig}
        >
          {loading ? '⏳ Running Comparison...' : '🎯 Run Model Comparison'}
        </button>
      </div>

      {error && (
        <div className="error-message">
          <span>⚠️ {error}</span>
        </div>
      )}

      {/* Comparison Results */}
      {comparisonResults && (
        <div className="comparison-results">
          {/* Side-by-Side Performance */}
          <div className="performance-comparison">
            <h3>📊 Performance Comparison</h3>
            <div className="performance-grid">
              <div className="performance-column">
                <h4>Baseline: {comparisonResults.baseline_config.name}</h4>
                <div className="metrics-list">
                  <div className="metric-item">
                    <span className="metric-label">Total Return</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.baseline_metrics.total_return)}</span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Sharpe Ratio</span>
                    <span className="metric-value">{formatDecimal(comparisonResults.baseline_metrics.sharpe_ratio)}</span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Max Drawdown</span>
                    <span className="metric-value warning">{formatPercentage(comparisonResults.baseline_metrics.max_drawdown)}</span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Win Rate</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.baseline_metrics.win_rate)}</span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Volatility</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.baseline_metrics.volatility)}</span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Profit Factor</span>
                    <span className="metric-value">{formatDecimal(comparisonResults.baseline_metrics.profit_factor)}</span>
                  </div>
                </div>
              </div>

              <div className="performance-column">
                <h4>Test: {comparisonResults.test_config.name}</h4>
                <div className="metrics-list">
                  <div className="metric-item">
                    <span className="metric-label">Total Return</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.test_metrics.total_return)}</span>
                    <span className={`metric-change ${comparisonResults.test_metrics.total_return > comparisonResults.baseline_metrics.total_return ? 'positive' : 'negative'}`}>
                      {comparisonResults.test_metrics.total_return > comparisonResults.baseline_metrics.total_return ? '↗' : '↘'} 
                      {formatPercentage(Math.abs(comparisonResults.test_metrics.total_return / comparisonResults.baseline_metrics.total_return - 1))}
                    </span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Sharpe Ratio</span>
                    <span className="metric-value">{formatDecimal(comparisonResults.test_metrics.sharpe_ratio)}</span>
                    <span className={`metric-change ${comparisonResults.test_metrics.sharpe_ratio > comparisonResults.baseline_metrics.sharpe_ratio ? 'positive' : 'negative'}`}>
                      {comparisonResults.test_metrics.sharpe_ratio > comparisonResults.baseline_metrics.sharpe_ratio ? '↗' : '↘'}
                      {formatPercentage(Math.abs(comparisonResults.test_metrics.sharpe_ratio / comparisonResults.baseline_metrics.sharpe_ratio - 1))}
                    </span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Max Drawdown</span>
                    <span className="metric-value warning">{formatPercentage(comparisonResults.test_metrics.max_drawdown)}</span>
                    <span className={`metric-change ${Math.abs(comparisonResults.test_metrics.max_drawdown) < Math.abs(comparisonResults.baseline_metrics.max_drawdown) ? 'positive' : 'negative'}`}>
                      {Math.abs(comparisonResults.test_metrics.max_drawdown) < Math.abs(comparisonResults.baseline_metrics.max_drawdown) ? '↗' : '↘'}
                      {formatPercentage(Math.abs(Math.abs(comparisonResults.test_metrics.max_drawdown) / Math.abs(comparisonResults.baseline_metrics.max_drawdown) - 1))}
                    </span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Win Rate</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.test_metrics.win_rate)}</span>
                    <span className={`metric-change ${comparisonResults.test_metrics.win_rate > comparisonResults.baseline_metrics.win_rate ? 'positive' : 'negative'}`}>
                      {comparisonResults.test_metrics.win_rate > comparisonResults.baseline_metrics.win_rate ? '↗' : '↘'}
                      {formatPercentage(Math.abs(comparisonResults.test_metrics.win_rate / comparisonResults.baseline_metrics.win_rate - 1))}
                    </span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Volatility</span>
                    <span className="metric-value">{formatPercentage(comparisonResults.test_metrics.volatility)}</span>
                    <span className={`metric-change ${comparisonResults.test_metrics.volatility < comparisonResults.baseline_metrics.volatility ? 'positive' : 'negative'}`}>
                      {comparisonResults.test_metrics.volatility < comparisonResults.baseline_metrics.volatility ? '↗' : '↘'}
                      {formatPercentage(Math.abs(comparisonResults.test_metrics.volatility / comparisonResults.baseline_metrics.volatility - 1))}
                    </span>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Profit Factor</span>
                    <span className="metric-value">{formatDecimal(comparisonResults.test_metrics.profit_factor)}</span>
                    <span className={`metric-change ${comparisonResults.test_metrics.profit_factor > comparisonResults.baseline_metrics.profit_factor ? 'positive' : 'negative'}`}>
                      {comparisonResults.test_metrics.profit_factor > comparisonResults.baseline_metrics.profit_factor ? '↗' : '↘'}
                      {formatPercentage(Math.abs(comparisonResults.test_metrics.profit_factor / comparisonResults.baseline_metrics.profit_factor - 1))}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Performance Chart Comparison */}
          <div className="chart-comparison">
            <h3>📈 Cumulative Performance Comparison</h3>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={comparisonResults.performance_comparison}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="day" stroke="#888" />
                <YAxis stroke="#888" tickFormatter={formatPercentage} />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1a2332', border: '1px solid #00d4ff', color: '#fff' }}
                  formatter={(value, name) => [
                    formatPercentage(value),
                    name === 'baseline' ? 'Baseline' : name === 'test' ? 'Test' : 'Difference'
                  ]}
                />
                <Legend />
                <Line 
                  type="monotone" 
                  dataKey="baseline" 
                  stroke="#00d4ff" 
                  strokeWidth={2}
                  name="Baseline"
                />
                <Line 
                  type="monotone" 
                  dataKey="test" 
                  stroke="#00ff88" 
                  strokeWidth={2}
                  name="Test"
                />
                <ReferenceLine y={0} stroke="#666" strokeDasharray="2 2" />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Statistical Significance */}
          <div className="statistical-analysis">
            <h3>📊 Statistical Significance Analysis</h3>
            <div className="stats-grid">
              <div className="stat-card">
                <div className="stat-label">P-Value</div>
                <div className={`stat-value ${comparisonResults.statistical_analysis.p_value < 0.05 ? 'significant' : 'not-significant'}`}>
                  {comparisonResults.statistical_analysis.p_value.toFixed(4)}
                </div>
                <div className="stat-interpretation">
                  {comparisonResults.statistical_analysis.is_significant ? '✅ Significant (p < 0.05)' : '❌ Not significant (p ≥ 0.05)'}
                </div>
              </div>

              <div className="stat-card">
                <div className="stat-label">Effect Size (Cohen's d)</div>
                <div className="stat-value">
                  {comparisonResults.statistical_analysis.cohens_d.toFixed(3)}
                </div>
                <div className="stat-interpretation">
                  {Math.abs(comparisonResults.statistical_analysis.cohens_d) > 0.8 ? '🚀 Large effect' : 
                   Math.abs(comparisonResults.statistical_analysis.cohens_d) > 0.5 ? '📊 Medium effect' : '📉 Small effect'}
                </div>
              </div>

              <div className="stat-card">
                <div className="stat-label">95% Confidence Interval</div>
                <div className="stat-value confidence-interval">
                  [{formatPercentage(comparisonResults.statistical_analysis.confidence_interval[0])}, {formatPercentage(comparisonResults.statistical_analysis.confidence_interval[1])}]
                </div>
                <div className="stat-interpretation">
                  {comparisonResults.statistical_analysis.confidence_interval[0] > 0 ? '✅ Consistently better' :
                   comparisonResults.statistical_analysis.confidence_interval[1] < 0 ? '❌ Consistently worse' : '⚖️ Mixed results'}
                </div>
              </div>
            </div>
          </div>

          {/* Recommendation */}
          <div className="recommendation-section">
            <h3>🎯 Model Selection Recommendation</h3>
            <div className="recommendation-card" style={{ borderColor: getRecommendationColor(comparisonResults.recommendation.decision) }}>
              <div className="recommendation-header">
                <span className="recommendation-icon">
                  {getRecommendationIcon(comparisonResults.recommendation.decision)}
                </span>
                <span className="recommendation-title" style={{ color: getRecommendationColor(comparisonResults.recommendation.decision) }}>
                  {comparisonResults.recommendation.decision.replace('_', ' ').toUpperCase()}
                </span>
                <span className="recommendation-confidence" style={{ color: getConfidenceColor(comparisonResults.recommendation.confidence) }}>
                  {comparisonResults.recommendation.confidence.toUpperCase()} CONFIDENCE
                </span>
              </div>

              <div className="recommendation-details">
                <div className="recommendation-reasons">
                  <h4>✅ Supporting Evidence:</h4>
                  <ul>
                    {comparisonResults.recommendation.reasons.map((reason, index) => (
                      <li key={index}>{reason}</li>
                    ))}
                  </ul>
                </div>

                {comparisonResults.recommendation.concerns.length > 0 && (
                  <div className="recommendation-concerns">
                    <h4>⚠️ Considerations:</h4>
                    <ul>
                      {comparisonResults.recommendation.concerns.map((concern, index) => (
                        <li key={index}>{concern}</li>
                      ))}
                    </ul>
                  </div>
                )}

                <div className="recommendation-next-steps">
                  <h4>📋 Recommended Next Steps:</h4>
                  <ol>
                    {comparisonResults.recommendation.next_steps.map((step, index) => (
                      <li key={index}>{step}</li>
                    ))}
                  </ol>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ModelComparisonDashboard;