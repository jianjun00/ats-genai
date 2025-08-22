// API Configuration
export const API_CONFIG = {
  BASE_URL: 'http://localhost:5001/api/v1',
  TIMEOUT: 10000, // 10 seconds
};

export const API_ENDPOINTS = {
  DATASETS: '/datasets',
  DATASET_BY_ID: (id: string) => `/datasets/${id}`,
  DISTRIBUTIONS: (id: string) => `/datasets/${id}/distributions`,
  SEQUENCE: (datasetId: string, sequenceId: string) => `/datasets/${datasetId}/sequences/${sequenceId}`,
  COMPARE: '/compare',
  HEALTH: '/health',
};