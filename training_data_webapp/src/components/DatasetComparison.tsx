import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Alert,
  LinearProgress,
  Tabs,
  Tab,
  Button
} from '@mui/material';
import Plot from 'react-plotly.js';
import { DatasetSummary, ComparisonResult, FeatureDistribution } from '../types/TrainingData';

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
      id={`comparison-tabpanel-${index}`}
      aria-labelledby={`comparison-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const DatasetComparison: React.FC = () => {
  const [searchParams] = useSearchParams();
  const [loading, setLoading] = useState(true);
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [selectedDataset1, setSelectedDataset1] = useState<string>('');
  const [selectedDataset2, setSelectedDataset2] = useState<string>('');
  const [comparisonResults, setComparisonResults] = useState<ComparisonResult[]>([]);
  const [tabValue, setTabValue] = useState(0);

  // Mock datasets
  const mockDatasets: DatasetSummary[] = [
    {
      id: 'dataset_20250820_143022',
      name: 'AAPL/MSFT 2-Symbol Training Set',
      creation_timestamp: '2025-08-20T14:30:22Z',
      total_sequences: 66,
      feature_count: 5,
      label_count: 2,
      symbols: ['AAPL', 'MSFT'],
      date_range: { start: '2025-06-30', end: '2025-08-18' },
      quality_score: 0.95,
      size_mb: 2.1
    },
    {
      id: 'dataset_20250820_120045',
      name: 'S&P 500 Technical Indicators',
      creation_timestamp: '2025-08-20T12:00:45Z',
      total_sequences: 2485,
      feature_count: 15,
      label_count: 3,
      symbols: ['SPY', 'QQQ', 'IWM'],
      date_range: { start: '2024-01-01', end: '2025-08-19' },
      quality_score: 0.88,
      size_mb: 45.7
    },
    {
      id: 'dataset_20250819_093011',
      name: 'Crypto Momentum Features',
      creation_timestamp: '2025-08-19T09:30:11Z',
      total_sequences: 1256,
      feature_count: 8,
      label_count: 1,
      symbols: ['BTC-USD', 'ETH-USD'],
      date_range: { start: '2023-01-01', end: '2025-08-18' },
      quality_score: 0.82,
      size_mb: 18.3
    }
  ];

  const mockComparisonResults: ComparisonResult[] = [
    {
      feature_name: 'returns_1d',
      dataset1_stats: { mean: 0.002, std: 0.018, min: -0.08, max: 0.12 },
      dataset2_stats: { mean: 0.0015, std: 0.022, min: -0.095, max: 0.15 },
      statistical_tests: {
        ks_test: { statistic: 0.12, p_value: 0.045, significant: true },
        t_test: { statistic: 1.85, p_value: 0.067, significant: false }
      },
      distribution_difference: 0.08
    },
    {
      feature_name: 'volatility_10d',
      dataset1_stats: { mean: 0.018, std: 0.008, min: 0.005, max: 0.045 },
      dataset2_stats: { mean: 0.022, std: 0.012, min: 0.008, max: 0.055 },
      statistical_tests: {
        ks_test: { statistic: 0.18, p_value: 0.012, significant: true },
        t_test: { statistic: 2.95, p_value: 0.003, significant: true }
      },
      distribution_difference: 0.15
    }
  ];

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      try {
        // Simulate API call
        await new Promise(resolve => setTimeout(resolve, 1000));
        setDatasets(mockDatasets);
        
        // Pre-select datasets from URL params
        const datasetsParam = searchParams.get('datasets');
        if (datasetsParam) {
          const [dataset1, dataset2] = datasetsParam.split(',');
          setSelectedDataset1(dataset1 || '');
          setSelectedDataset2(dataset2 || '');
        }
      } catch (error) {
        console.error('Failed to load datasets:', error);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [searchParams]);

  useEffect(() => {
    const runComparison = async () => {
      if (selectedDataset1 && selectedDataset2 && selectedDataset1 !== selectedDataset2) {
        // Simulate comparison API call
        await new Promise(resolve => setTimeout(resolve, 500));
        setComparisonResults(mockComparisonResults);
      } else {
        setComparisonResults([]);
      }
    };

    runComparison();
  }, [selectedDataset1, selectedDataset2]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const renderOverviewComparison = () => {
    const dataset1 = datasets.find(d => d.id === selectedDataset1);
    const dataset2 = datasets.find(d => d.id === selectedDataset2);

    if (!dataset1 || !dataset2) return null;

    return (
      <Grid container spacing={3}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Dataset Overview Comparison
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Metric</TableCell>
                      <TableCell>{dataset1.name}</TableCell>
                      <TableCell>{dataset2.name}</TableCell>
                      <TableCell>Difference</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <TableRow>
                      <TableCell>Total Sequences</TableCell>
                      <TableCell>{dataset1.total_sequences.toLocaleString()}</TableCell>
                      <TableCell>{dataset2.total_sequences.toLocaleString()}</TableCell>
                      <TableCell>
                        <Chip 
                          label={`${((dataset2.total_sequences - dataset1.total_sequences) / dataset1.total_sequences * 100).toFixed(1)}%`}
                          color={dataset2.total_sequences > dataset1.total_sequences ? 'success' : 'error'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Feature Count</TableCell>
                      <TableCell>{dataset1.feature_count}</TableCell>
                      <TableCell>{dataset2.feature_count}</TableCell>
                      <TableCell>
                        <Chip 
                          label={`${dataset2.feature_count - dataset1.feature_count > 0 ? '+' : ''}${dataset2.feature_count - dataset1.feature_count}`}
                          color={dataset2.feature_count > dataset1.feature_count ? 'success' : dataset2.feature_count < dataset1.feature_count ? 'error' : 'default'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Label Count</TableCell>
                      <TableCell>{dataset1.label_count}</TableCell>
                      <TableCell>{dataset2.label_count}</TableCell>
                      <TableCell>
                        <Chip 
                          label={`${dataset2.label_count - dataset1.label_count > 0 ? '+' : ''}${dataset2.label_count - dataset1.label_count}`}
                          color={dataset2.label_count > dataset1.label_count ? 'success' : dataset2.label_count < dataset1.label_count ? 'error' : 'default'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Quality Score</TableCell>
                      <TableCell>
                        <Chip 
                          label={`${(dataset1.quality_score * 100).toFixed(0)}%`}
                          color={dataset1.quality_score > 0.9 ? 'success' : dataset1.quality_score > 0.8 ? 'warning' : 'error'}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={`${(dataset2.quality_score * 100).toFixed(0)}%`}
                          color={dataset2.quality_score > 0.9 ? 'success' : dataset2.quality_score > 0.8 ? 'warning' : 'error'}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={`${((dataset2.quality_score - dataset1.quality_score) * 100).toFixed(1)}%`}
                          color={dataset2.quality_score > dataset1.quality_score ? 'success' : 'error'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Date Range</TableCell>
                      <TableCell>{dataset1.date_range.start} to {dataset1.date_range.end}</TableCell>
                      <TableCell>{dataset2.date_range.start} to {dataset2.date_range.end}</TableCell>
                      <TableCell>
                        {new Date(dataset2.date_range.end).getTime() - new Date(dataset2.date_range.start).getTime() >
                         new Date(dataset1.date_range.end).getTime() - new Date(dataset1.date_range.start).getTime()
                          ? 'Longer' : 'Shorter'} range
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Size</TableCell>
                      <TableCell>{dataset1.size_mb.toFixed(1)} MB</TableCell>
                      <TableCell>{dataset2.size_mb.toFixed(1)} MB</TableCell>
                      <TableCell>
                        <Chip 
                          label={`${((dataset2.size_mb - dataset1.size_mb) / dataset1.size_mb * 100).toFixed(1)}%`}
                          color={dataset2.size_mb > dataset1.size_mb ? 'warning' : 'success'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Symbols Comparison
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <Box>
                  <Typography variant="body2" color="textSecondary">
                    {dataset1.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 1 }}>
                    {dataset1.symbols.map(symbol => (
                      <Chip key={symbol} label={symbol} size="small" color="primary" />
                    ))}
                  </Box>
                </Box>
                <Box>
                  <Typography variant="body2" color="textSecondary">
                    {dataset2.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 1 }}>
                    {dataset2.symbols.map(symbol => (
                      <Chip 
                        key={symbol} 
                        label={symbol} 
                        size="small" 
                        color={dataset1.symbols.includes(symbol) ? 'success' : 'secondary'}
                      />
                    ))}
                  </Box>
                </Box>
                <Box>
                  <Typography variant="body2" color="textSecondary">
                    Common Symbols
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 1 }}>
                    {dataset1.symbols.filter(symbol => dataset2.symbols.includes(symbol)).map(symbol => (
                      <Chip key={symbol} label={symbol} size="small" color="success" />
                    ))}
                    {dataset1.symbols.filter(symbol => dataset2.symbols.includes(symbol)).length === 0 && (
                      <Typography variant="body2" color="textSecondary">
                        No common symbols
                      </Typography>
                    )}
                  </Box>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Size Comparison
              </Typography>
              <Plot
                data={[
                  {
                    values: [dataset1.size_mb, dataset2.size_mb],
                    labels: [dataset1.name, dataset2.name],
                    type: 'pie',
                    marker: {
                      colors: ['#667eea', '#764ba2']
                    },
                    textinfo: 'label+percent',
                    textposition: 'outside'
                  }
                ]}
                layout={{
                  title: 'Dataset Size Distribution',
                  paper_bgcolor: 'rgba(0,0,0,0)',
                  plot_bgcolor: 'rgba(0,0,0,0)',
                  font: { color: 'white' },
                  margin: { t: 60, r: 20, b: 20, l: 20 },
                  showlegend: false
                }}
                style={{ width: '100%', height: '300px' }}
                config={{ displayModeBar: false }}
              />
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    );
  };

  const renderFeatureComparison = () => {
    if (comparisonResults.length === 0) {
      return (
        <Alert severity="info">
          Select two different datasets to see feature comparison.
        </Alert>
      );
    }

    return (
      <Grid container spacing={3}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Feature Statistical Comparison
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Feature</TableCell>
                      <TableCell>Dataset 1 Mean</TableCell>
                      <TableCell>Dataset 2 Mean</TableCell>
                      <TableCell>Std Dev Difference</TableCell>
                      <TableCell>KS Test</TableCell>
                      <TableCell>T-Test</TableCell>
                      <TableCell>Distribution Diff</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {comparisonResults.map((result) => (
                      <TableRow key={result.feature_name}>
                        <TableCell>
                          <Typography variant="body2" sx={{ fontWeight: 500 }}>
                            {result.feature_name}
                          </Typography>
                        </TableCell>
                        <TableCell>{result.dataset1_stats.mean.toFixed(4)}</TableCell>
                        <TableCell>{result.dataset2_stats.mean.toFixed(4)}</TableCell>
                        <TableCell>
                          <Chip
                            label={`${((result.dataset2_stats.std - result.dataset1_stats.std) / result.dataset1_stats.std * 100).toFixed(1)}%`}
                            color={Math.abs(result.dataset2_stats.std - result.dataset1_stats.std) / result.dataset1_stats.std > 0.1 ? 'warning' : 'success'}
                            size="small"
                          />
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={`p=${result.statistical_tests.ks_test.p_value.toFixed(3)}`}
                            color={result.statistical_tests.ks_test.significant ? 'error' : 'success'}
                            size="small"
                          />
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={`p=${result.statistical_tests.t_test.p_value.toFixed(3)}`}
                            color={result.statistical_tests.t_test.significant ? 'error' : 'success'}
                            size="small"
                          />
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={`${(result.distribution_difference * 100).toFixed(1)}%`}
                            color={result.distribution_difference > 0.1 ? 'warning' : 'success'}
                            size="small"
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        {/* Distribution Comparison Charts */}
        {comparisonResults.map((result) => (
          <Grid item xs={12} lg={6} key={result.feature_name}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {result.feature_name} Distribution Comparison
                </Typography>
                <Plot
                  data={[
                    {
                      x: Array.from({ length: 50 }, (_, i) => 
                        result.dataset1_stats.min + (i / 49) * (result.dataset1_stats.max - result.dataset1_stats.min)
                      ),
                      y: Array.from({ length: 50 }, (_, i) => {
                        const x = result.dataset1_stats.min + (i / 49) * (result.dataset1_stats.max - result.dataset1_stats.min);
                        return Math.exp(-0.5 * Math.pow((x - result.dataset1_stats.mean) / result.dataset1_stats.std, 2));
                      }),
                      type: 'scatter',
                      mode: 'lines',
                      name: 'Dataset 1',
                      line: { color: '#667eea', width: 2 }
                    },
                    {
                      x: Array.from({ length: 50 }, (_, i) => 
                        result.dataset2_stats.min + (i / 49) * (result.dataset2_stats.max - result.dataset2_stats.min)
                      ),
                      y: Array.from({ length: 50 }, (_, i) => {
                        const x = result.dataset2_stats.min + (i / 49) * (result.dataset2_stats.max - result.dataset2_stats.min);
                        return Math.exp(-0.5 * Math.pow((x - result.dataset2_stats.mean) / result.dataset2_stats.std, 2));
                      }),
                      type: 'scatter',
                      mode: 'lines',
                      name: 'Dataset 2',
                      line: { color: '#764ba2', width: 2 }
                    }
                  ]}
                  layout={{
                    xaxis: { title: 'Value' },
                    yaxis: { title: 'Density' },
                    paper_bgcolor: 'rgba(0,0,0,0)',
                    plot_bgcolor: 'rgba(0,0,0,0)',
                    font: { color: 'white' },
                    margin: { t: 40, r: 20, b: 40, l: 60 },
                    legend: { orientation: 'h', y: -0.2 }
                  }}
                  style={{ width: '100%', height: '300px' }}
                  config={{ displayModeBar: false }}
                />
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
    );
  };

  if (loading) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Dataset Comparison
        </Typography>
        <Card>
          <CardContent>
            <LinearProgress />
            <Typography sx={{ mt: 2, textAlign: 'center' }}>
              Loading datasets...
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
        Dataset Comparison
      </Typography>

      {/* Dataset Selectors */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={3} alignItems="center">
            <Grid item xs={12} md={5}>
              <FormControl fullWidth>
                <InputLabel>Dataset 1</InputLabel>
                <Select
                  value={selectedDataset1}
                  onChange={(e) => setSelectedDataset1(e.target.value)}
                  label="Dataset 1"
                >
                  {datasets.map((dataset) => (
                    <MenuItem key={dataset.id} value={dataset.id}>
                      {dataset.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2} sx={{ textAlign: 'center' }}>
              <Typography variant="h6" color="textSecondary">
                vs
              </Typography>
            </Grid>
            <Grid item xs={12} md={5}>
              <FormControl fullWidth>
                <InputLabel>Dataset 2</InputLabel>
                <Select
                  value={selectedDataset2}
                  onChange={(e) => setSelectedDataset2(e.target.value)}
                  label="Dataset 2"
                >
                  {datasets.map((dataset) => (
                    <MenuItem key={dataset.id} value={dataset.id}>
                      {dataset.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {selectedDataset1 && selectedDataset2 && selectedDataset1 !== selectedDataset2 ? (
        <>
          {/* Comparison Tabs */}
          <Card sx={{ mb: 3 }}>
            <Tabs value={tabValue} onChange={handleTabChange}>
              <Tab label="Overview" />
              <Tab label="Feature Statistics" />
            </Tabs>
          </Card>

          {/* Tab Content */}
          <TabPanel value={tabValue} index={0}>
            {renderOverviewComparison()}
          </TabPanel>

          <TabPanel value={tabValue} index={1}>
            {renderFeatureComparison()}
          </TabPanel>
        </>
      ) : (
        <Alert severity="info">
          Please select two different datasets to compare.
        </Alert>
      )}
    </Box>
  );
};

export default DatasetComparison;