import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  LinearProgress,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper
} from '@mui/material';
import Plot from 'react-plotly.js';
import { TrainingSequence, FeatureMetadata, LabelMetadata } from '../types/TrainingData';

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
      id={`sequence-tabpanel-${index}`}
      aria-labelledby={`sequence-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const TrainingSequenceViewer: React.FC = () => {
  const { datasetId, sequenceId } = useParams<{ datasetId: string; sequenceId: string }>();
  const [loading, setLoading] = useState(true);
  const [sequence, setSequence] = useState<TrainingSequence | null>(null);
  const [featureMetadata, setFeatureMetadata] = useState<FeatureMetadata[]>([]);
  const [labelMetadata, setLabelMetadata] = useState<LabelMetadata[]>([]);
  const [selectedSequenceIndex, setSelectedSequenceIndex] = useState<number>(0);
  const [availableSequences, setAvailableSequences] = useState<number[]>([]);
  const [tabValue, setTabValue] = useState(0);

  // Mock data - in real app, this would come from an API
  const mockSequence: TrainingSequence = {
    id: 'dataset_20250820_143022_sample_000001',
    dataset_id: 'dataset_20250820_143022',
    sequence_index: 1,
    features: [
      // 15 time steps, 5 features
      Array(15).fill(0).map((_, t) => [
        0.002 + Math.random() * 0.01 - 0.005,  // returns_1d
        0.001 + Math.random() * 0.008 - 0.004, // returns_2d_lag
        0.0015 + Math.random() * 0.009 - 0.0045, // log_returns
        0.015 + Math.random() * 0.01,           // volatility_10d
        0.8 + Math.random() * 0.4               // volume_ratio
      ])
    ],
    labels: [
      // 3 prediction steps, 2 labels
      Array(3).fill(0).map((_, t) => [
        0.003 + Math.random() * 0.01 - 0.005,  // future_return_1d
        Math.random() > 0.5 ? 1 : 0            // price_direction_1d
      ])
    ],
    feature_masks: [Array(15).fill(0).map(() => Array(5).fill(true))],
    label_masks: [Array(3).fill(0).map(() => Array(2).fill(true))],
    symbol: 'AAPL',
    start_date: '2025-07-15',
    end_date: '2025-08-05'
  };

  const mockFeatureMetadata: FeatureMetadata[] = [
    {
      name: 'returns_1d_pct_change_lag1',
      feature_type: 'return',
      data_type: 'float64',
      dimension: 1,
      description: 'Return calculation with 1 period lag',
      visualization_type: 'line_chart',
      min_value: -0.08,
      max_value: 0.12,
      mean_value: 0.002,
      std_value: 0.018,
      null_count: 0,
      is_primary_key: false,
      parameters: {}
    },
    {
      name: 'returns_2d_lag_pct_change_lag2',
      feature_type: 'return',
      data_type: 'float64',
      dimension: 1,
      description: 'Return calculation with 2 period lag',
      visualization_type: 'line_chart',
      min_value: -0.08,
      max_value: 0.12,
      mean_value: 0.001,
      std_value: 0.016,
      null_count: 0,
      is_primary_key: false,
      parameters: {}
    },
    {
      name: 'log_returns_log_return_lag1',
      feature_type: 'return',
      data_type: 'float64',
      dimension: 1,
      description: 'Log return calculation with 1 period lag',
      visualization_type: 'line_chart',
      min_value: -0.08,
      max_value: 0.12,
      mean_value: 0.0015,
      std_value: 0.017,
      null_count: 0,
      is_primary_key: false,
      parameters: {}
    },
    {
      name: 'volatility_10d_volatility',
      feature_type: 'price_indicator',
      data_type: 'float64',
      dimension: 1,
      description: 'Price volatility using 10 period window',
      visualization_type: 'line_chart',
      min_value: 0.005,
      max_value: 0.045,
      mean_value: 0.018,
      std_value: 0.008,
      null_count: 0,
      is_primary_key: false,
      parameters: {}
    },
    {
      name: 'volume_ratio_volume_ratio',
      feature_type: 'volume_indicator',
      data_type: 'float64',
      dimension: 1,
      description: 'Volume ratio indicator',
      visualization_type: 'line_chart',
      min_value: 0.3,
      max_value: 2.5,
      mean_value: 1.0,
      std_value: 0.3,
      null_count: 0,
      is_primary_key: false,
      parameters: {}
    }
  ];

  const mockLabelMetadata: LabelMetadata[] = [
    {
      name: 'future_return_1d_simple_lead1',
      label_type: 'return',
      data_type: 'float64',
      dimension: 1,
      description: 'Simple return label predicting 1 periods ahead',
      lead_periods: 1,
      parameters: {},
      visualization_type: 'line_chart',
      min_value: -0.08,
      max_value: 0.12
    },
    {
      name: 'price_direction_1d_direction_lead1',
      label_type: 'classification',
      data_type: 'float64',
      dimension: 1,
      description: 'Classification label predicting 1 periods ahead',
      lead_periods: 1,
      parameters: {},
      visualization_type: 'bar_chart',
      min_value: 0,
      max_value: 1,
      unique_values: [0, 1],
      class_distribution: { '0': 30, '1': 36 }
    }
  ];

  useEffect(() => {
    const loadSequence = async () => {
      setLoading(true);
      try {
        // Simulate API call
        await new Promise(resolve => setTimeout(resolve, 800));
        setSequence(mockSequence);
        setFeatureMetadata(mockFeatureMetadata);
        setLabelMetadata(mockLabelMetadata);
        // Generate available sequence indices (mock)
        setAvailableSequences(Array.from({ length: 66 }, (_, i) => i));
        setSelectedSequenceIndex(parseInt(sequenceId || '0'));
      } catch (error) {
        console.error('Failed to load sequence:', error);
      } finally {
        setLoading(false);
      }
    };

    loadSequence();
  }, [datasetId, sequenceId]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleSequenceChange = (newSequenceIndex: number) => {
    setSelectedSequenceIndex(newSequenceIndex);
    // In real app, would navigate to new URL or reload data
  };

  const renderFeatureTimeSeries = () => {
    if (!sequence || !featureMetadata) return null;

    const timeSteps = Array.from({ length: sequence.features[0].length }, (_, i) => i);
    
    const traces = featureMetadata.map((feature, featureIdx) => ({
      x: timeSteps,
      y: sequence.features[0].map(step => step[featureIdx]),
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: feature.name,
      line: {
        width: 2
      },
      marker: {
        size: 4
      }
    }));

    return (
      <Plot
        data={traces}
        layout={{
          title: 'Feature Values Over Time',
          xaxis: { 
            title: 'Time Step',
            showgrid: true,
            gridcolor: 'rgba(255,255,255,0.1)'
          },
          yaxis: { 
            title: 'Feature Value',
            showgrid: true,
            gridcolor: 'rgba(255,255,255,0.1)'
          },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          legend: {
            orientation: 'h',
            y: -0.2
          },
          margin: { t: 60, r: 20, b: 100, l: 80 }
        }}
        style={{ width: '100%', height: '500px' }}
        config={{ displayModeBar: true }}
      />
    );
  };

  const renderLabelPredictions = () => {
    if (!sequence || !labelMetadata) return null;

    const predictionSteps = Array.from({ length: sequence.labels[0].length }, (_, i) => i + 1);
    
    const traces = labelMetadata.map((label, labelIdx) => ({
      x: predictionSteps,
      y: sequence.labels[0].map(step => step[labelIdx]),
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: label.name,
      line: {
        width: 3
      },
      marker: {
        size: 8
      }
    }));

    return (
      <Plot
        data={traces}
        layout={{
          title: 'Label Predictions',
          xaxis: { 
            title: 'Prediction Step',
            showgrid: true,
            gridcolor: 'rgba(255,255,255,0.1)'
          },
          yaxis: { 
            title: 'Label Value',
            showgrid: true,
            gridcolor: 'rgba(255,255,255,0.1)'
          },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          legend: {
            orientation: 'h',
            y: -0.2
          },
          margin: { t: 60, r: 20, b: 100, l: 80 }
        }}
        style={{ width: '100%', height: '400px' }}
        config={{ displayModeBar: true }}
      />
    );
  };

  const renderFeatureHeatmap = () => {
    if (!sequence || !featureMetadata) return null;

    const featureNames = featureMetadata.map(f => f.name);
    const timeSteps = Array.from({ length: sequence.features[0].length }, (_, i) => `T-${sequence.features[0].length - 1 - i}`);
    
    // Normalize features for heatmap
    const normalizedData = sequence.features[0].map((step, timeIdx) => 
      step.map((value, featureIdx) => {
        const metadata = featureMetadata[featureIdx];
        if (metadata.max_value && metadata.min_value) {
          return (value - metadata.min_value!) / (metadata.max_value! - metadata.min_value!);
        }
        return value;
      })
    );

    return (
      <Plot
        data={[
          {
            z: normalizedData,
            x: featureNames,
            y: timeSteps.reverse(),
            type: 'heatmap',
            colorscale: 'Viridis',
            showscale: true,
            colorbar: {
              title: 'Normalized Value'
            }
          }
        ]}
        layout={{
          title: 'Feature Values Heatmap (Normalized)',
          xaxis: { 
            title: 'Features',
            tickangle: 45
          },
          yaxis: { 
            title: 'Time Steps (Relative)'
          },
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          font: { color: 'white' },
          margin: { t: 60, r: 20, b: 120, l: 80 }
        }}
        style={{ width: '100%', height: '500px' }}
        config={{ displayModeBar: true }}
      />
    );
  };

  const renderDataTable = () => {
    if (!sequence || !featureMetadata || !labelMetadata) return null;

    return (
      <Grid container spacing={3}>
        <Grid item xs={12} lg={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Feature Values by Time Step
              </Typography>
              <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Time Step</TableCell>
                      {featureMetadata.map((feature) => (
                        <TableCell key={feature.name} align="right">
                          {feature.name.split('_')[0]}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {sequence.features[0].map((step, timeIdx) => (
                      <TableRow key={timeIdx}>
                        <TableCell>T-{sequence.features[0].length - 1 - timeIdx}</TableCell>
                        {step.map((value, featureIdx) => (
                          <TableCell key={featureIdx} align="right">
                            {value.toFixed(4)}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} lg={4}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Label Predictions
              </Typography>
              <TableContainer component={Paper}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Step</TableCell>
                      {labelMetadata.map((label) => (
                        <TableCell key={label.name} align="right">
                          {label.name.split('_')[0]}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {sequence.labels[0].map((step, stepIdx) => (
                      <TableRow key={stepIdx}>
                        <TableCell>T+{stepIdx + 1}</TableCell>
                        {step.map((value, labelIdx) => (
                          <TableCell key={labelIdx} align="right">
                            {labelMetadata[labelIdx].label_type === 'classification' 
                              ? value.toFixed(0) 
                              : value.toFixed(4)
                            }
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    );
  };

  if (loading) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Training Sequence Viewer
        </Typography>
        <Card>
          <CardContent>
            <LinearProgress />
            <Typography sx={{ mt: 2, textAlign: 'center' }}>
              Loading training sequence...
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (!sequence) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Training Sequence Viewer
        </Typography>
        <Alert severity="error">Failed to load training sequence</Alert>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
        Training Sequence Viewer
      </Typography>

      {/* Sequence Selector and Info */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={3} alignItems="center">
            <Grid item xs={12} md={4}>
              <FormControl fullWidth>
                <InputLabel>Sequence Index</InputLabel>
                <Select
                  value={selectedSequenceIndex}
                  onChange={(e) => handleSequenceChange(e.target.value as number)}
                  label="Sequence Index"
                >
                  {availableSequences.map((idx) => (
                    <MenuItem key={idx} value={idx}>
                      Sequence {idx + 1}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            
            <Grid item xs={12} md={8}>
              <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>
                <Chip label={`Symbol: ${sequence.symbol}`} color="primary" />
                <Chip label={`Features: ${featureMetadata.length}`} variant="outlined" />
                <Chip label={`Labels: ${labelMetadata.length}`} variant="outlined" />
                <Chip label={`Steps: ${sequence.features[0].length}`} variant="outlined" />
                <Typography variant="body2" color="textSecondary">
                  {sequence.start_date} to {sequence.end_date}
                </Typography>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Visualization Tabs */}
      <Card sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab label="Time Series" />
          <Tab label="Heatmap" />
          <Tab label="Predictions" />
          <Tab label="Data Table" />
        </Tabs>
      </Card>

      {/* Tab Content */}
      <TabPanel value={tabValue} index={0}>
        <Card>
          <CardContent>
            {renderFeatureTimeSeries()}
          </CardContent>
        </Card>
      </TabPanel>

      <TabPanel value={tabValue} index={1}>
        <Card>
          <CardContent>
            {renderFeatureHeatmap()}
          </CardContent>
        </Card>
      </TabPanel>

      <TabPanel value={tabValue} index={2}>
        <Card>
          <CardContent>
            {renderLabelPredictions()}
          </CardContent>
        </Card>
      </TabPanel>

      <TabPanel value={tabValue} index={3}>
        {renderDataTable()}
      </TabPanel>
    </Box>
  );
};

export default TrainingSequenceViewer;