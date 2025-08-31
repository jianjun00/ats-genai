import React, { useState, useEffect } from 'react';
import { 
  LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, 
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, ScatterPlot, Histogram 
} from 'recharts';
import './EnhancedEDADashboard.css';

const EnhancedEDADashboard = () => {
  const [activeTopTab, setActiveTopTab] = useState('table'); // 'table' or 'training-dataset'
  const [tableData, setTableData] = useState([]);
  const [trainingDatasets, setTrainingDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [datasetDistributions, setDatasetDistributions] = useState(null);
  const [histogramData, setHistogramData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  // Table EDA state
  const [selectedTable, setSelectedTable] = useState('');
  const [availableTables, setAvailableTables] = useState([]);
  const [tableDistributions, setTableDistributions] = useState(null);
  
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';

  // Fetch data on component mount
  useEffect(() => {
    if (activeTopTab === 'table') {
      fetchAvailableTables();
    } else if (activeTopTab === 'training-dataset') {
      fetchTrainingDatasets();
    }
  }, [activeTopTab]);

  // Table EDA functions
  const fetchAvailableTables = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/v1/datasets`);
      if (!response.ok) throw new Error('Failed to fetch tables');
      const data = await response.json();
      setAvailableTables(data.datasets || []);
    } catch (err) {
      setError(`Failed to load tables: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const fetchTableDistributions = async (tableName) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/v1/eda/distributions/${tableName}`);
      if (!response.ok) throw new Error('Failed to fetch table distributions');
      const data = await response.json();
      setTableDistributions(data);
    } catch (err) {
      setError(`Failed to load table distributions: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    fetchTableDistributions(tableName);
  };

  // Training Dataset EDA functions
  const fetchTrainingDatasets = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/v1/training-datasets/`);
      if (!response.ok) throw new Error('Failed to fetch training datasets');
      const data = await response.json();
      setTrainingDatasets(data.datasets || []);
    } catch (err) {
      setError(`Failed to load training datasets: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const fetchDatasetDistributions = async (datasetId) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/v1/training-datasets/${datasetId}/distributions`);
      if (!response.ok) throw new Error('Failed to fetch dataset distributions');
      const data = await response.json();
      setDatasetDistributions(data);
    } catch (err) {
      setError(`Failed to load dataset distributions: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const fetchHistogramData = async (datasetId, featureName = null) => {
    try {
      setLoading(true);
      const url = featureName 
        ? `${API_BASE_URL}/api/v1/training-datasets/${datasetId}/histogram?feature_name=${featureName}`
        : `${API_BASE_URL}/api/v1/training-datasets/${datasetId}/histogram`;
      
      const response = await fetch(url);
      if (!response.ok) throw new Error('Failed to fetch histogram data');
      const data = await response.json();
      setHistogramData(data);
    } catch (err) {
      setError(`Failed to load histogram data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDatasetSelect = (dataset) => {
    setSelectedDataset(dataset);
    fetchDatasetDistributions(dataset.id);
    fetchHistogramData(dataset.id);
  };

  // Render functions
  const renderTabNavigation = () => (
    <div className="eda-tab-navigation">
      <button 
        className={`eda-tab ${activeTopTab === 'table' ? 'active' : ''}`}
        onClick={() => setActiveTopTab('table')}
      >
        📊 Table EDA
      </button>
      <button 
        className={`eda-tab ${activeTopTab === 'training-dataset' ? 'active' : ''}`}
        onClick={() => setActiveTopTab('training-dataset')}
      >
        🧠 Training Dataset EDA
      </button>
    </div>
  );

  const renderTableEDA = () => (
    <div className="table-eda-section">
      <h2>Table Exploratory Data Analysis</h2>
      
      {/* Table Selection */}
      <div className="table-selector">
        <label>Select Table:</label>
        <select 
          value={selectedTable} 
          onChange={(e) => handleTableSelect(e.target.value)}
          className="table-dropdown"
        >
          <option value="">-- Select a Table --</option>
          {availableTables.map(table => (
            <option key={table.name} value={table.name}>
              {table.name} ({table.row_count?.toLocaleString() || 0} rows)
            </option>
          ))}
        </select>
      </div>

      {/* Table EDA Results */}
      {selectedTable && tableDistributions && (
        <div className="table-eda-results">
          <h3>Table: {selectedTable}</h3>
          
          {/* Column Statistics */}
          <div className="column-stats">
            <h4>Column Statistics</h4>
            <div className="stats-grid">
              {Object.entries(tableDistributions.columns || {}).map(([colName, stats]) => (
                <div key={colName} className="column-stat-card">
                  <h5>{colName}</h5>
                  <div className="stat-details">
                    <p>Type: {stats.data_type}</p>
                    <p>Non-null: {stats.non_null_count?.toLocaleString()}</p>
                    <p>Unique: {stats.unique_count?.toLocaleString()}</p>
                    {stats.mean && <p>Mean: {stats.mean.toFixed(2)}</p>}
                    {stats.std && <p>Std: {stats.std.toFixed(2)}</p>}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Distribution Visualizations */}
          <div className="distribution-charts">
            <h4>Data Distributions</h4>
            <div className="charts-grid">
              {Object.entries(tableDistributions.distributions || {}).map(([colName, distData]) => (
                <div key={colName} className="chart-container">
                  <h5>{colName} Distribution</h5>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={distData.histogram || []}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="bin" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="count" fill="#8884d8" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );

  const renderTrainingDatasetEDA = () => (
    <div className="training-dataset-eda-section">
      <h2>Training Dataset Exploratory Data Analysis</h2>
      
      {/* Dataset Selection */}
      <div className="dataset-selector">
        <h3>Available Training Datasets</h3>
        <div className="dataset-grid">
          {trainingDatasets.map(dataset => (
            <div 
              key={dataset.id} 
              className={`dataset-card ${selectedDataset?.id === dataset.id ? 'selected' : ''}`}
              onClick={() => handleDatasetSelect(dataset)}
            >
              <h4>{dataset.dataset_name}</h4>
              <div className="dataset-info">
                <p>📊 Sequences: {dataset.total_sequences?.toLocaleString()}</p>
                <p>🔗 Features: {dataset.feature_count}</p>
                <p>🎯 Labels: {dataset.label_count}</p>
                <p>📈 Quality: {(dataset.data_quality_score * 100).toFixed(1)}%</p>
                <p>📅 Range: {dataset.date_range_start} to {dataset.date_range_end}</p>
                <p>💾 Size: {dataset.file_size_mb?.toFixed(1)} MB</p>
              </div>
              {dataset.technical_indicators && (
                <div className="technical-indicators">
                  <p>🔧 Indicators: {dataset.technical_indicators}</p>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Selected Dataset Analysis */}
      {selectedDataset && datasetDistributions && (
        <div className="dataset-analysis">
          <h3>Dataset Analysis: {selectedDataset.dataset_name}</h3>
          
          {/* TFDV Statistics Summary */}
          <div className="tfdv-summary">
            <h4>Data Validation Summary</h4>
            <div className="stats-cards">
              <div className="stat-card">
                <h5>Data Quality Score</h5>
                <div className="quality-score">
                  {(datasetDistributions.data_quality_score * 100).toFixed(1)}%
                </div>
              </div>
              <div className="stat-card">
                <h5>Feature Completeness</h5>
                <div className="completeness-score">
                  {(datasetDistributions.feature_completeness * 100).toFixed(1)}%
                </div>
              </div>
              <div className="stat-card">
                <h5>Label Completeness</h5>
                <div className="completeness-score">
                  {(datasetDistributions.label_completeness * 100).toFixed(1)}%
                </div>
              </div>
            </div>
          </div>

          {/* Feature Distributions */}
          {datasetDistributions.feature_distributions && (
            <div className="feature-distributions">
              <h4>Feature Distributions</h4>
              <div className="distribution-grid">
                {Object.entries(datasetDistributions.feature_distributions).map(([featureName, dist]) => (
                  <div key={featureName} className="feature-dist-card">
                    <h5>{featureName}</h5>
                    {dist.type === 'numeric' && (
                      <div className="numeric-stats">
                        <p>Mean: {dist.mean?.toFixed(4)}</p>
                        <p>Std: {dist.std?.toFixed(4)}</p>
                        <p>Range: [{dist.min?.toFixed(4)}, {dist.max?.toFixed(4)}]</p>
                        
                        {/* Mini histogram */}
                        {histogramData && histogramData.tfdv_statistics?.features?.[featureName] && (
                          <div className="mini-histogram">
                            <ResponsiveContainer width="100%" height={100}>
                              <BarChart data={[/* histogram data would go here */]}>
                                <XAxis hide />
                                <YAxis hide />
                                <Bar dataKey="count" fill="#82ca9d" />
                              </BarChart>
                            </ResponsiveContainer>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Label Distributions */}
          {datasetDistributions.label_distributions && (
            <div className="label-distributions">
              <h4>Label Distributions</h4>
              <div className="distribution-grid">
                {Object.entries(datasetDistributions.label_distributions).map(([labelName, dist]) => (
                  <div key={labelName} className="label-dist-card">
                    <h5>{labelName}</h5>
                    {dist.type === 'numeric' && (
                      <div className="numeric-stats">
                        <p>Mean: {dist.mean?.toFixed(4)}</p>
                        <p>Std: {dist.std?.toFixed(4)}</p>
                        <p>Range: [{dist.min?.toFixed(4)}, {dist.max?.toFixed(4)}]</p>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* TFDV Anomalies */}
          {datasetDistributions.tfdv_statistics?.anomalies && 
           Object.keys(datasetDistributions.tfdv_statistics.anomalies).length > 0 && (
            <div className="tfdv-anomalies">
              <h4>⚠️ Data Anomalies Detected</h4>
              <div className="anomalies-list">
                {Object.entries(datasetDistributions.tfdv_statistics.anomalies).map(([field, anomaly]) => (
                  <div key={field} className="anomaly-card">
                    <h5>{field}</h5>
                    <p>{anomaly.description || 'Anomaly detected'}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );

  // Main render
  return (
    <div className="enhanced-eda-dashboard">
      <div className="eda-header">
        <h1>🔬 ATS Exploratory Data Analysis</h1>
        <p>Comprehensive data exploration and validation for tables and training datasets</p>
      </div>

      {renderTabNavigation()}

      {loading && <div className="loading-spinner">Loading...</div>}
      
      {error && (
        <div className="error-message">
          <strong>Error:</strong> {error}
        </div>
      )}

      <div className="eda-content">
        {activeTopTab === 'table' && renderTableEDA()}
        {activeTopTab === 'training-dataset' && renderTrainingDatasetEDA()}
      </div>
    </div>
  );
};

export default EnhancedEDADashboard;