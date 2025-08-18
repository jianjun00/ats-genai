import { useState, useEffect, useCallback } from 'react';
import useWebSocket from 'react-use-websocket';
import { apiClient } from '../services/apiClient';

// Custom hook for portfolio data management
export const usePortfolioData = (backtestId, timeRange, benchmarkId) => {
  const [portfolioData, setPortfolioData] = useState(null);
  const [attributionData, setAttributionData] = useState(null);
  const [modelData, setModelData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  // Format date range for API
  const formatDateRange = useCallback((range) => {
    if (!range || range.length !== 2) return {};
    return {
      start_date: range[0].format('YYYY-MM-DD'),
      end_date: range[1].format('YYYY-MM-DD')
    };
  }, []);
  
  // Fetch portfolio metrics
  const fetchPortfolioMetrics = useCallback(async () => {
    try {
      const dateParams = formatDateRange(timeRange);
      const params = {
        ...dateParams,
        ...(benchmarkId && { benchmark_id: benchmarkId })
      };
      
      const response = await apiClient.get(`/api/v1/backtests/${backtestId}/portfolio/metrics`, {
        params
      });
      
      return response.data;
    } catch (err) {
      console.error('Failed to fetch portfolio metrics:', err);
      throw err;
    }
  }, [backtestId, timeRange, benchmarkId, formatDateRange]);
  
  // Fetch portfolio performance time series
  const fetchPortfolioPerformance = useCallback(async () => {
    try {
      const dateParams = formatDateRange(timeRange);
      const response = await apiClient.get(`/api/v1/backtests/${backtestId}/portfolio/performance`, {
        params: dateParams
      });
      
      return response.data;
    } catch (err) {
      console.error('Failed to fetch portfolio performance:', err);
      throw err;
    }
  }, [backtestId, timeRange, formatDateRange]);
  
  // Fetch attribution data
  const fetchAttributionData = useCallback(async () => {
    try {
      const dateParams = formatDateRange(timeRange);
      const response = await apiClient.get(`/api/v1/backtests/${backtestId}/attribution`, {
        params: dateParams
      });
      
      return response.data;
    } catch (err) {
      console.error('Failed to fetch attribution data:', err);
      throw err;
    }
  }, [backtestId, timeRange, formatDateRange]);
  
  // Fetch model performance data
  const fetchModelData = useCallback(async () => {
    try {
      const dateParams = formatDateRange(timeRange);
      const [performanceResponse, forecastsResponse] = await Promise.all([
        apiClient.get(`/api/v1/backtests/${backtestId}/model/performance`, {
          params: dateParams
        }),
        apiClient.get(`/api/v1/backtests/${backtestId}/forecasts`, {
          params: dateParams
        })
      ]);
      
      return {
        performance: performanceResponse.data,
        forecasts: forecastsResponse.data
      };
    } catch (err) {
      console.error('Failed to fetch model data:', err);
      throw err;
    }
  }, [backtestId, timeRange, formatDateRange]);
  
  // Fetch benchmark data if specified
  const fetchBenchmarkData = useCallback(async () => {
    if (!benchmarkId) return null;
    
    try {
      const dateParams = formatDateRange(timeRange);
      const response = await apiClient.get(`/api/v1/backtests/${benchmarkId}/portfolio/performance`, {
        params: dateParams
      });
      
      return response.data;
    } catch (err) {
      console.error('Failed to fetch benchmark data:', err);
      return null;
    }
  }, [benchmarkId, timeRange, formatDateRange]);
  
  // Main fetch function
  const fetchAllData = useCallback(async () => {
    if (!backtestId) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const [metrics, performance, attribution, model, benchmark] = await Promise.all([
        fetchPortfolioMetrics(),
        fetchPortfolioPerformance(),
        fetchAttributionData(),
        fetchModelData(),
        fetchBenchmarkData()
      ]);
      
      setPortfolioData({
        metrics,
        performance,
        benchmark
      });
      
      setAttributionData(attribution);
      setModelData(model);
      
    } catch (err) {
      setError(err);
      console.error('Failed to fetch dashboard data:', err);
    } finally {
      setLoading(false);
    }
  }, [
    backtestId,
    fetchPortfolioMetrics,
    fetchPortfolioPerformance,
    fetchAttributionData,
    fetchModelData,
    fetchBenchmarkData
  ]);
  
  // Refetch function for manual refresh
  const refetch = useCallback(() => {
    return fetchAllData();
  }, [fetchAllData]);
  
  // Effect to fetch data when dependencies change
  useEffect(() => {
    fetchAllData();
  }, [fetchAllData]);
  
  return {
    portfolioData,
    attributionData,
    modelData,
    loading,
    error,
    refetch
  };
};

// Custom hook for real-time updates
export const useRealTimeUpdates = (backtestId, enabled = false) => {
  const [realTimeData, setRealTimeData] = useState({
    isConnected: false,
    latestMetrics: null,
    latestPerformance: null,
    lastUpdate: null
  });
  
  // WebSocket URL
  const socketUrl = enabled && backtestId 
    ? `ws://localhost:8000/ws/backtests/${backtestId}/portfolio`
    : null;
  
  const {
    sendMessage,
    lastMessage,
    readyState
  } = useWebSocket(socketUrl, {
    onOpen: () => {
      console.log('WebSocket connected');
      setRealTimeData(prev => ({ ...prev, isConnected: true }));
    },
    onClose: () => {
      console.log('WebSocket disconnected');
      setRealTimeData(prev => ({ ...prev, isConnected: false }));
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
      setRealTimeData(prev => ({ ...prev, isConnected: false }));
    },
    shouldReconnect: (closeEvent) => {
      // Reconnect on disconnect
      return true;
    },
    reconnectAttempts: 10,
    reconnectInterval: 3000
  });
  
  // Process incoming WebSocket messages
  useEffect(() => {
    if (lastMessage !== null) {
      try {
        const message = JSON.parse(lastMessage.data);
        
        switch (message.type) {
          case 'portfolio_metrics':
            setRealTimeData(prev => ({
              ...prev,
              latestMetrics: message.data,
              lastUpdate: message.timestamp
            }));
            break;
            
          case 'portfolio_update':
            setRealTimeData(prev => ({
              ...prev,
              latestPerformance: message.data,
              lastUpdate: message.timestamp
            }));
            break;
            
          case 'error':
            console.error('WebSocket error message:', message.message);
            break;
            
          default:
            console.log('Unknown message type:', message.type);
        }
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    }
  }, [lastMessage]);
  
  // Request data update
  const requestUpdate = useCallback(() => {
    if (readyState === 1) { // WebSocket.OPEN
      sendMessage(JSON.stringify({
        type: 'request_update',
        timestamp: new Date().toISOString()
      }));
    }
  }, [sendMessage, readyState]);
  
  return {
    ...realTimeData,
    requestUpdate
  };
};

// Custom hook for drill-down data
export const useDrillDownData = () => {
  const [drillDownData, setDrillDownData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const fetchDrillDownData = useCallback(async (backtestId, drillDownInfo) => {
    if (!backtestId || !drillDownInfo) return;
    
    setLoading(true);
    setError(null);
    
    try {
      let endpoint;
      let params = {};
      
      switch (drillDownInfo.type) {
        case 'time_period':
          endpoint = `/api/v1/backtests/${backtestId}/drill-down/period`;
          params = {
            start_date: drillDownInfo.period.start,
            end_date: drillDownInfo.period.end
          };
          break;
          
        case 'stock':
          endpoint = `/api/v1/backtests/${backtestId}/drill-down/stock/${drillDownInfo.symbol}`;
          break;
          
        case 'trade':
          endpoint = `/api/v1/backtests/${backtestId}/drill-down/trade/${drillDownInfo.tradeId}`;
          break;
          
        default:
          throw new Error(`Unknown drill-down type: ${drillDownInfo.type}`);
      }
      
      const response = await apiClient.get(endpoint, { params });
      setDrillDownData(response.data);
      
    } catch (err) {
      setError(err);
      console.error('Failed to fetch drill-down data:', err);
    } finally {
      setLoading(false);
    }
  }, []);
  
  const clearDrillDownData = useCallback(() => {
    setDrillDownData(null);
    setError(null);
  }, []);
  
  return {
    drillDownData,
    loading,
    error,
    fetchDrillDownData,
    clearDrillDownData
  };
};

// Custom hook for comparison data
export const useComparisonData = () => {
  const [comparisonData, setComparisonData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const fetchPortfolioComparison = useCallback(async (backtestIds, dateRange) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await apiClient.post('/api/v1/comparison/portfolio', {
        backtest_run_ids: backtestIds,
        start_date: dateRange?.start_date,
        end_date: dateRange?.end_date
      });
      
      setComparisonData(response.data);
    } catch (err) {
      setError(err);
      console.error('Failed to fetch portfolio comparison:', err);
    } finally {
      setLoading(false);
    }
  }, []);
  
  const fetchModelComparison = useCallback(async (backtestIds, dateRange) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await apiClient.post('/api/v1/comparison/models', {
        backtest_run_ids: backtestIds,
        start_date: dateRange?.start_date,
        end_date: dateRange?.end_date
      });
      
      setComparisonData(response.data);
    } catch (err) {
      setError(err);
      console.error('Failed to fetch model comparison:', err);
    } finally {
      setLoading(false);
    }
  }, []);
  
  return {
    comparisonData,
    loading,
    error,
    fetchPortfolioComparison,
    fetchModelComparison
  };
};