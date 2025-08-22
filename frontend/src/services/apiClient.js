import axios from 'axios';

// Create API client instance
const apiClient = axios.create({
  baseURL: process.env.REACT_APP_API_URL || 'http://localhost:8000',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add auth tokens if needed
apiClient.interceptors.request.use(
  (config) => {
    // Add auth token if available
    const token = localStorage.getItem('authToken');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    
    // Add request timestamp for debugging
    config.metadata = { startTime: new Date() };
    
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for error handling and logging
apiClient.interceptors.response.use(
  (response) => {
    // Calculate request duration
    const endTime = new Date();
    const duration = endTime - response.config.metadata.startTime;
    
    // Log successful requests in development
    if (process.env.NODE_ENV === 'development') {
      console.log(`API Request: ${response.config.method?.toUpperCase()} ${response.config.url} - ${duration}ms`);
    }
    
    return response;
  },
  (error) => {
    // Calculate request duration for failed requests
    if (error.config?.metadata?.startTime) {
      const endTime = new Date();
      const duration = endTime - error.config.metadata.startTime;
      console.error(`API Request Failed: ${error.config.method?.toUpperCase()} ${error.config.url} - ${duration}ms`);
    }
    
    // Handle different error types
    if (error.response) {
      // Server responded with error status
      const { status, data } = error.response;
      
      switch (status) {
        case 401:
          // Unauthorized - redirect to login
          localStorage.removeItem('authToken');
          window.location.href = '/login';
          break;
          
        case 403:
          // Forbidden
          console.error('Access forbidden:', data.detail || 'Insufficient permissions');
          break;
          
        case 404:
          // Not found
          console.error('Resource not found:', data.detail || 'The requested resource was not found');
          break;
          
        case 422:
          // Validation error
          console.error('Validation error:', data.detail || 'Invalid request data');
          break;
          
        case 500:
          // Server error
          console.error('Server error:', data.detail || 'Internal server error');
          break;
          
        default:
          console.error('API Error:', status, data.detail || 'Unknown error');
      }
      
      // Return formatted error
      return Promise.reject({
        status,
        message: data.detail || `HTTP ${status} Error`,
        data: data
      });
      
    } else if (error.request) {
      // Network error
      console.error('Network error:', error.message);
      return Promise.reject({
        status: 0,
        message: 'Network error - please check your connection',
        data: null
      });
      
    } else {
      // Other error
      console.error('Request error:', error.message);
      return Promise.reject({
        status: -1,
        message: error.message,
        data: null
      });
    }
  }
);

// API methods
export const apiMethods = {
  // Backtest management
  async getBacktests(params = {}) {
    const response = await apiClient.get('/api/v1/backtests', { params });
    return response.data;
  },
  
  // Portfolio analytics
  async getPortfolioMetrics(backtestId, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/portfolio/metrics`, { params });
    return response.data;
  },
  
  async getPortfolioPerformance(backtestId, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/portfolio/performance`, { params });
    return response.data;
  },
  
  async getAttribution(backtestId, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/attribution`, { params });
    return response.data;
  },
  
  // Model performance
  async getModelPerformance(backtestId, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/model/performance`, { params });
    return response.data;
  },
  
  async getForecasts(backtestId, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/forecasts`, { params });
    return response.data;
  },
  
  // Comparisons
  async comparePortfolios(request) {
    const response = await apiClient.post('/api/v1/comparison/portfolio', request);
    return response.data;
  },
  
  async compareModels(request) {
    const response = await apiClient.post('/api/v1/comparison/models', request);
    return response.data;
  },
  
  // Drill-down analysis
  async getDrillDownPeriod(backtestId, params) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/drill-down/period`, { params });
    return response.data;
  },
  
  async getDrillDownStock(backtestId, symbol, params = {}) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/drill-down/stock/${symbol}`, { params });
    return response.data;
  },
  
  async getDrillDownTrade(backtestId, tradeId) {
    const response = await apiClient.get(`/api/v1/backtests/${backtestId}/drill-down/trade/${tradeId}`);
    return response.data;
  },
  
  // Cache management
  async invalidateCache(pattern = null) {
    const params = pattern ? { pattern } : {};
    const response = await apiClient.post('/api/v1/cache/invalidate', null, { params });
    return response.data;
  }
};

// Helper functions for common API patterns
export const apiHelpers = {
  // Format date range for API calls
  formatDateRange(startDate, endDate) {
    return {
      start_date: startDate?.format('YYYY-MM-DD'),
      end_date: endDate?.format('YYYY-MM-DD')
    };
  },
  
  // Build query parameters
  buildParams(baseParams, additionalParams = {}) {
    return { ...baseParams, ...additionalParams };
  },
  
  // Handle pagination
  buildPaginationParams(page = 1, pageSize = 50) {
    return {
      offset: (page - 1) * pageSize,
      limit: pageSize
    };
  },
  
  // Retry logic for failed requests
  async retryRequest(requestFn, maxRetries = 3, delay = 1000) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await requestFn();
      } catch (error) {
        lastError = error;
        
        if (attempt < maxRetries) {
          console.log(`Request failed, retrying in ${delay}ms (attempt ${attempt}/${maxRetries})`);
          await new Promise(resolve => setTimeout(resolve, delay));
          delay *= 2; // Exponential backoff
        }
      }
    }
    
    throw lastError;
  }
};

// WebSocket helper for real-time connections
export const websocketHelpers = {
  // Create WebSocket URL
  createWebSocketUrl(endpoint, backtestId) {
    const baseUrl = process.env.REACT_APP_WS_URL || 'ws://localhost:8000';
    return `${baseUrl}/ws/backtests/${backtestId}/${endpoint}`;
  },
  
  // WebSocket message handlers
  createMessageHandler(onMessage, onError) {
    return (event) => {
      try {
        const message = JSON.parse(event.data);
        onMessage(message);
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
        onError?.(error);
      }
    };
  }
};

export { apiClient };