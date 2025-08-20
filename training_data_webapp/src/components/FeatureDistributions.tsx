import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Tabs,
  Tab,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Alert,
  LinearProgress
} from '@mui/material';
import Plot from 'react-plotly.js';
import { FeatureMetadata, FeatureDistribution, OHLCData } from '../types/TrainingData';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`feature-tabpanel-${index}`}
      aria-labelledby={`feature-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const FeatureDistributions: React.FC = () => {
  const { datasetId } = useParams<{ datasetId: string }>();
  const [loading, setLoading] = useState(true);
  const [features, setFeatures] = useState<FeatureMetadata[]>([]);
  const [distributions, setDistributions] = useState<Record<string, FeatureDistribution>>({});
  const [selectedFeature, setSelectedFeature] = useState<string>('');
  const [tabValue, setTabValue] = useState(0);
  const [ohlcData, setOhlcData] = useState<OHLCData[]>([]);

  // Mock data - in real app, this would come from an API
  const mockFeatures: FeatureMetadata[] = [
    {
      name: 'returns_1d_pct_change_lag1',
      feature_type: 'return',
      data_type: 'float64',
      dimension: 1,
      description: 'Return calculation with 1 period lag',
      source_column: 'close',
      lag_periods: 1,
      parameters: { transform_type: 'pct_change', column: 'close', periods: 1 },
      visualization_type: 'distribution',
      min_value: -0.08,
      max_value: 0.12,
      mean_value: 0.002,
      std_value: 0.018,
      null_count: 0,
      is_primary_key: false
    },
    {
      name: 'volatility_10d_volatility',
      feature_type: 'price_indicator',
      data_type: 'float64',
      dimension: 1,
      description: 'Price-based technical indicator using 10 period window',
      source_column: 'close',
      window_size: 10,
      parameters: { transform_type: 'volatility', column: 'close', window: 10 },
      visualization_type: 'line_chart',
      min_value: 0.005,
      max_value: 0.045,
      mean_value: 0.018,
      std_value: 0.008,
      null_count: 0,
      is_primary_key: false
    },
    {
      name: 'ohlc_data',
      feature_type: 'ohlc',
      data_type: 'float64',
      dimension: 4,
      description: 'OHLC price data',
      parameters: {},
      visualization_type: 'candlestick',
      min_value: 95.5,
      max_value: 110.2,
      mean_value: 102.5,
      std_value: 3.8,
      null_count: 0,
      is_primary_key: false
    }
  ];

  const mockDistributions: Record<string, FeatureDistribution> = {
    'returns_1d_pct_change_lag1': {
      feature_name: 'returns_1d_pct_change_lag1',
      feature_type: 'return',
      histogram: {
        bins: [-0.08, -0.06, -0.04, -0.02, 0, 0.02, 0.04, 0.06, 0.08, 0.10, 0.12],
        counts: [2, 5, 12, 18, 25, 22, 15, 8, 3, 1, 0]
      },
      statistics: {
        mean: 0.002,
        std: 0.018,
        min: -0.08,
        max: 0.12,
        percentiles: {
          p25: -0.010,
          p50: 0.001,
          p75: 0.015,
          p90: 0.028,
          p95: 0.038,
          p99: 0.075
        }
      },
      time_series: {
        timestamps: ['2025-07-01', '2025-07-02', '2025-07-03', '2025-07-04', '2025-07-05'],
        values: [0.012, -0.008, 0.025, -0.015, 0.032]
      }
    },
    'volatility_10d_volatility': {
      feature_name: 'volatility_10d_volatility',
      feature_type: 'price_indicator',
      histogram: {
        bins: [0.005, 0.010, 0.015, 0.020, 0.025, 0.030, 0.035, 0.040, 0.045],
        counts: [3, 8, 15, 20, 18, 12, 8, 5, 2]
      },
      statistics: {
        mean: 0.018,
        std: 0.008,
        min: 0.005,
        max: 0.045,
        percentiles: {
          p25: 0.012,
          p50: 0.017,
          p75: 0.024,
          p90: 0.032,
          p95: 0.038,
          p99: 0.043
        }
      },
      time_series: {
        timestamps: ['2025-07-01', '2025-07-02', '2025-07-03', '2025-07-04', '2025-07-05'],
        values: [0.015, 0.018, 0.022, 0.019, 0.016]
      }
    }
  };

  const mockOHLCData: OHLCData[] = [
    { timestamp: '2025-07-01', open: 100.0, high: 101.5, low: 99.5, close: 101.0, volume: 1000000 },
    { timestamp: '2025-07-02', open: 101.0, high: 102.0, low: 100.0, close: 101.5, volume: 1200000 },
    { timestamp: '2025-07-03', open: 101.5, high: 103.0, low: 101.0, close: 102.8, volume: 950000 },
    { timestamp: '2025-07-04', open: 102.8, high: 103.5, low: 101.5, close: 102.0, volume: 1100000 },
    { timestamp: '2025-07-05', open: 102.0, high: 104.0, low: 101.8, close: 103.5, volume: 1300000 }
  ];

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      try {
        // Simulate API calls
        await new Promise(resolve => setTimeout(resolve, 1000));
        setFeatures(mockFeatures);
        setDistributions(mockDistributions);
        setOhlcData(mockOHLCData);
        if (mockFeatures.length > 0) {
          setSelectedFeature(mockFeatures[0].name);
        }
      } catch (error) {
        console.error('Failed to load feature data:', error);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [datasetId]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const renderHistogram = (distribution: FeatureDistribution) => {
    return (
      <Plot
        data={[
          {
            x: distribution.histogram.bins.slice(0, -1),
            y: distribution.histogram.counts,
            type: 'bar',
            marker: {
              color: 'rgba(102, 126, 234, 0.7)',
              line: {
                color: 'rgba(102, 126, 234, 1)',
                width: 1
              }
            },
            name: 'Distribution'
          }
        ]}
        layout={{
          title: `${distribution.feature_name} Distribution`,
          xaxis: { title: 'Value' },
          yaxis: { title: 'Frequency' },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          margin: { t: 40, r: 20, b: 40, l: 60 }
        }}
        style={{ width: '100%', height: '400px' }}
        config={{ displayModeBar: false }}
      />
    );
  };

  const renderTimeSeries = (distribution: FeatureDistribution) => {
    if (!distribution.time_series) return null;

    return (
      <Plot
        data={[
          {
            x: distribution.time_series.timestamps,
            y: distribution.time_series.values,
            type: 'scatter',
            mode: 'lines+markers',
            line: {
              color: '#667eea',
              width: 2
            },
            marker: {
              color: '#667eea',
              size: 6
            },
            name: distribution.feature_name
          }
        ]}
        layout={{
          title: `${distribution.feature_name} Time Series`,
          xaxis: { title: 'Date' },
          yaxis: { title: 'Value' },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          margin: { t: 40, r: 20, b: 40, l: 60 }
        }}
        style={{ width: '100%', height: '400px' }}
        config={{ displayModeBar: false }}
      />
    );
  };

  const renderCandlestick = () => {
    return (
      <Plot
        data={[
          {
            x: ohlcData.map(d => d.timestamp),
            open: ohlcData.map(d => d.open),
            high: ohlcData.map(d => d.high),
            low: ohlcData.map(d => d.low),
            close: ohlcData.map(d => d.close),
            type: 'candlestick',
            increasing: { line: { color: '#00d4aa' } },
            decreasing: { line: { color: '#ff6b6b' } },
            name: 'OHLC'
          }
        ]}
        layout={{
          title: 'OHLC Candlestick Chart',
          xaxis: { 
            title: 'Date',
            rangeslider: { visible: false }
          },
          yaxis: { title: 'Price' },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          margin: { t: 40, r: 20, b: 40, l: 60 }
        }}
        style={{ width: '100%', height: '500px' }}
        config={{ displayModeBar: true }}
      />
    );
  };

  const renderVolumeChart = () => {
    return (
      <Plot
        data={[
          {
            x: ohlcData.map(d => d.timestamp),
            y: ohlcData.map(d => d.volume || 0),
            type: 'bar',
            marker: {
              color: 'rgba(118, 75, 162, 0.7)',
              line: {
                color: 'rgba(118, 75, 162, 1)',
                width: 1
              }
            },
            name: 'Volume'
          }
        ]}
        layout={{
          title: 'Volume Chart',
          xaxis: { title: 'Date' },
          yaxis: { title: 'Volume' },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          margin: { t: 40, r: 20, b: 40, l: 60 }
        }}
        style={{ width: '100%', height: '300px' }}
        config={{ displayModeBar: false }}
      />
    );
  };

  const renderStatistics = (distribution: FeatureDistribution) => {
    return (
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Statistical Summary
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">Mean</Typography>
              <Typography variant="h6">{distribution.statistics.mean.toFixed(4)}</Typography>
            </Grid>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">Std Dev</Typography>
              <Typography variant="h6">{distribution.statistics.std.toFixed(4)}</Typography>
            </Grid>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">Min</Typography>
              <Typography variant="h6">{distribution.statistics.min.toFixed(4)}</Typography>
            </Grid>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">Max</Typography>
              <Typography variant="h6">{distribution.statistics.max.toFixed(4)}</Typography>
            </Grid>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">25th %ile</Typography>
              <Typography variant="h6">{distribution.statistics.percentiles.p25.toFixed(4)}</Typography>
            </Grid>
            <Grid item xs={6} sm={4}>
              <Typography variant="body2" color="textSecondary">75th %ile</Typography>
              <Typography variant="h6">{distribution.statistics.percentiles.p75.toFixed(4)}</Typography>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    );
  };

  if (loading) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Feature Distributions
        </Typography>
        <Card>
          <CardContent>
            <LinearProgress />
            <Typography sx={{ mt: 2, textAlign: 'center' }}>
              Loading feature distributions...
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  const selectedFeatureData = features.find(f => f.name === selectedFeature);
  const selectedDistribution = distributions[selectedFeature];

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
        Feature Distributions - Dataset {datasetId}
      </Typography>

      {/* Feature Selector */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Select Feature</InputLabel>
                <Select
                  value={selectedFeature}
                  onChange={(e) => setSelectedFeature(e.target.value)}
                  label="Select Feature"
                >
                  {features.map((feature) => (
                    <MenuItem key={feature.name} value={feature.name}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Chip 
                          label={feature.feature_type} 
                          size="small" 
                          color="primary" 
                          variant="outlined" 
                        />
                        {feature.name}
                      </Box>
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              {selectedFeatureData && (
                <Typography variant="body2" color="textSecondary">
                  {selectedFeatureData.description}
                </Typography>
              )}
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {selectedFeatureData?.feature_type === 'ohlc' ? (
        // OHLC specific view
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Card>
              <CardContent>
                {renderCandlestick()}
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12}>
            <Card>
              <CardContent>
                {renderVolumeChart()}
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      ) : (
        // Regular feature view
        <>
          <Card sx={{ mb: 3 }}>
            <Tabs value={tabValue} onChange={handleTabChange}>
              <Tab label="Distribution" />
              <Tab label="Time Series" />
              <Tab label="Statistics" />
            </Tabs>
          </Card>

          <Grid container spacing={3}>
            <Grid item xs={12} lg={8}>
              <Card>
                <CardContent>
                  <TabPanel value={tabValue} index={0}>
                    {selectedDistribution && renderHistogram(selectedDistribution)}
                  </TabPanel>
                  <TabPanel value={tabValue} index={1}>
                    {selectedDistribution && renderTimeSeries(selectedDistribution)}
                  </TabPanel>
                  <TabPanel value={tabValue} index={2}>
                    {selectedDistribution && renderStatistics(selectedDistribution)}
                  </TabPanel>
                </CardContent>
              </Card>
            </Grid>
            
            <Grid item xs={12} lg={4}>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Feature Details
                  </Typography>
                  {selectedFeatureData && (
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                      <Box>
                        <Typography variant="body2" color="textSecondary">Type</Typography>
                        <Chip label={selectedFeatureData.feature_type} size="small" color="primary" />
                      </Box>
                      <Box>
                        <Typography variant="body2" color="textSecondary">Data Type</Typography>
                        <Typography variant="body1">{selectedFeatureData.data_type}</Typography>
                      </Box>
                      <Box>
                        <Typography variant="body2" color="textSecondary">Dimension</Typography>
                        <Typography variant="body1">{selectedFeatureData.dimension}</Typography>
                      </Box>
                      {selectedFeatureData.lag_periods && (
                        <Box>
                          <Typography variant="body2" color="textSecondary">Lag Periods</Typography>
                          <Typography variant="body1">{selectedFeatureData.lag_periods}</Typography>
                        </Box>
                      )}
                      {selectedFeatureData.window_size && (
                        <Box>
                          <Typography variant="body2" color="textSecondary">Window Size</Typography>
                          <Typography variant="body1">{selectedFeatureData.window_size}</Typography>
                        </Box>
                      )}
                      <Box>
                        <Typography variant="body2" color="textSecondary">Null Count</Typography>
                        <Typography variant="body1">{selectedFeatureData.null_count}</Typography>
                      </Box>
                    </Box>
                  )}
                </CardContent>
              </Card>
            </Grid>
          </Grid>
        </>
      )}
    </Box>
  );
};

export default FeatureDistributions;