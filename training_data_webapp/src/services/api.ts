import { API_CONFIG, API_ENDPOINTS } from '../config';
import { DatasetSummary, TrainingDataMetadata, FeatureDistribution, TrainingSequence, ComparisonResult } from '../types/TrainingData';

class ApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = API_CONFIG.BASE_URL;
  }

  private async request<T>(endpoint: string): Promise<T> {
    try {
      const response = await fetch(`${this.baseUrl}${endpoint}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      if (!data.success) {
        throw new Error(data.error || 'API request failed');
      }
      return data.data;
    } catch (error) {
      console.error(`API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  async getDatasets(): Promise<DatasetSummary[]> {
    return this.request<DatasetSummary[]>(API_ENDPOINTS.DATASETS);
  }

  async getDataset(id: string): Promise<TrainingDataMetadata> {
    return this.request<TrainingDataMetadata>(API_ENDPOINTS.DATASET_BY_ID(id));
  }

  async getDistributions(id: string): Promise<Record<string, FeatureDistribution>> {
    return this.request<Record<string, FeatureDistribution>>(API_ENDPOINTS.DISTRIBUTIONS(id));
  }

  async getSequence(datasetId: string, sequenceId: string): Promise<TrainingSequence> {
    return this.request<TrainingSequence>(API_ENDPOINTS.SEQUENCE(datasetId, sequenceId));
  }

  async compareDatasets(dataset1Id: string, dataset2Id: string): Promise<{comparison_results: ComparisonResult[]}> {
    return this.request<{comparison_results: ComparisonResult[]}>(`${API_ENDPOINTS.COMPARE}?dataset1=${dataset1Id}&dataset2=${dataset2Id}`);
  }

  async healthCheck(): Promise<any> {
    return this.request<any>(API_ENDPOINTS.HEALTH);
  }
}

export const apiService = new ApiService();
export default ApiService;