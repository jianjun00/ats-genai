import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Alert,
  LinearProgress,
  Divider,
  List,
  ListItem,
  ListItemText,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  Analytics as AnalyticsIcon,
  Visibility as ViewIcon,
  Download as DownloadIcon,
  Timeline as TimelineIcon,
  CompareArrows as CompareIcon
} from '@mui/icons-material';
import { TrainingDataMetadata } from '../types/TrainingData';

const DatasetDetails: React.FC = () => {
  const { datasetId } = useParams<{ datasetId: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [metadata, setMetadata] = useState<TrainingDataMetadata | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Mock metadata - in real app, this would come from an API
  const mockMetadata: TrainingDataMetadata = {
    dataset_name: 'dataset_20250820_143022',
    creation_timestamp: '2025-08-20T14:30:22Z',
    total_sequences: 66,
    sequence_length: 15,
    prediction_horizon: 3,
    feature_count: 5,
    label_count: 2,
    features: [
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
      }
    ],
    labels: [
      {
        name: 'future_return_1d_simple_lead1',
        label_type: 'return',
        data_type: 'float64',
        dimension: 1,
        description: 'Simple return label predicting 1 periods ahead',
        lead_periods: 1,
        parameters: { return_type: 'simple', column: 'close' },
        visualization_type: 'distribution',
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
        parameters: { class_type: 'direction', column: 'close' },
        visualization_type: 'bar_chart',
        min_value: 0,
        max_value: 1,
        unique_values: [0, 1],
        class_distribution: { '0': 30, '1': 36 }
      }
    ],
    symbols: ['AAPL', 'MSFT'],
    date_range: {
      start: '2025-06-30',
      end: '2025-08-18'
    },
    data_sources: ['polygon', 'internal'],
    data_file_path: '/training_data_output/dataset_20250820_143022_features.npy',
    feature_file_path: '/training_data_output/dataset_20250820_143022_features.npy',
    label_file_path: '/training_data_output/dataset_20250820_143022_labels.npy',
    sample_ids: Array.from({ length: 66 }, (_, i) => `dataset_20250820_143022_sample_${i.toString().padStart(6, '0')}`),
    primary_key_feature: 'returns_1d_pct_change_lag1',
    primary_key_type: 'float',
    gin_config_path: 'config/configurable_training_simple.gin',
    generation_parameters: {
      sequence_length: 15,
      prediction_horizon: 3,
      normalize_features: true,
      normalize_labels: false,
      remove_outliers: true,
      min_valid_ratio: 0.6,
      output_format: 'pytorch'
    },
    data_quality_metrics: {
      feature_missing_ratio: 0.02,
      label_missing_ratio: 0.01,
      overall_missing_ratio: 0.015,
      feature_completeness: 0.98,
      label_completeness: 0.99
    },
    outlier_count: 3,
    missing_data_ratio: 0.015
  };

  useEffect(() => {
    const loadMetadata = async () => {
      setLoading(true);
      try {
        // Simulate API call
        await new Promise(resolve => setTimeout(resolve, 800));
        setMetadata(mockMetadata);
      } catch (err) {
        setError('Failed to load dataset metadata');
      } finally {
        setLoading(false);
      }
    };

    loadMetadata();
  }, [datasetId]);

  const formatDate = (isoString: string) => {
    return new Date(isoString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getFeatureTypeColor = (type: string) => {
    const colors: Record<string, 'primary' | 'secondary' | 'success' | 'warning' | 'error' | 'info'> = {
      'return': 'primary',
      'price_indicator': 'secondary',
      'volume_indicator': 'info',
      'ohlc': 'success',
      'classification': 'warning',
      'binary': 'error'
    };
    return colors[type] || 'primary';
  };

  const calculateQualityScore = (metrics: Record<string, number>) => {
    const completeness = (metrics.feature_completeness + metrics.label_completeness) / 2;
    const missingPenalty = metrics.overall_missing_ratio * 0.5;
    return Math.max(0, Math.min(1, completeness - missingPenalty));
  };

  if (loading) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Dataset Details
        </Typography>
        <Card>
          <CardContent>
            <LinearProgress />
            <Typography sx={{ mt: 2, textAlign: 'center' }}>
              Loading dataset metadata...
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (error || !metadata) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Dataset Details
        </Typography>
        <Alert severity="error">{error || 'Dataset not found'}</Alert>
      </Box>
    );
  }

  const qualityScore = calculateQualityScore(metadata.data_quality_metrics);

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
        Dataset Details
      </Typography>

      {/* Header Info */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={3} alignItems="center">
            <Grid item xs={12} md={8}>
              <Typography variant="h5" gutterBottom>
                {metadata.dataset_name}
              </Typography>
              <Typography variant="body2" color="textSecondary" gutterBottom>
                Created: {formatDate(metadata.creation_timestamp)}
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mt: 2 }}>
                {metadata.symbols.map(symbol => (
                  <Chip key={symbol} label={symbol} color="primary" size="small" />
                ))}
                <Chip label={`${metadata.total_sequences} sequences`} variant="outlined" size="small" />
                <Chip label={`Quality: ${(qualityScore * 100).toFixed(0)}%`} 
                      color={qualityScore > 0.9 ? 'success' : qualityScore > 0.8 ? 'warning' : 'error'} 
                      size="small" />
              </Box>
            </Grid>
            <Grid item xs={12} md={4}>
              <Box sx={{ display: 'flex', gap: 1, flexDirection: 'column' }}>
                <Button
                  variant="contained"
                  startIcon={<AnalyticsIcon />}
                  onClick={() => navigate(`/distributions/${datasetId}`)}
                  fullWidth
                >
                  View Distributions
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<TimelineIcon />}
                  onClick={() => navigate(`/sequence/${datasetId}/0`)}
                  fullWidth
                >
                  View Sequences
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<CompareIcon />}
                  fullWidth
                >
                  Compare Dataset
                </Button>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      <Grid container spacing={3}>
        {/* Basic Information */}
        <Grid item xs={12} lg={6}>
          <Card sx={{ height: 'fit-content' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Basic Information
              </Typography>
              <List dense>
                <ListItem>
                  <ListItemText 
                    primary="Sequence Length" 
                    secondary={`${metadata.sequence_length} time steps`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Prediction Horizon" 
                    secondary={`${metadata.prediction_horizon} steps ahead`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Date Range" 
                    secondary={`${metadata.date_range.start} to ${metadata.date_range.end}`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Data Sources" 
                    secondary={metadata.data_sources.join(', ')} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Primary Key Feature" 
                    secondary={metadata.primary_key_feature || 'None'} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Gin Config" 
                    secondary={metadata.gin_config_path || 'Not specified'} 
                  />
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Data Quality Metrics */}
        <Grid item xs={12} lg={6}>
          <Card sx={{ height: 'fit-content' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Data Quality Metrics
              </Typography>
              <List dense>
                <ListItem>
                  <ListItemText 
                    primary="Feature Completeness" 
                    secondary={`${(metadata.data_quality_metrics.feature_completeness * 100).toFixed(1)}%`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Label Completeness" 
                    secondary={`${(metadata.data_quality_metrics.label_completeness * 100).toFixed(1)}%`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Overall Missing Data" 
                    secondary={`${(metadata.data_quality_metrics.overall_missing_ratio * 100).toFixed(2)}%`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Outlier Count" 
                    secondary={`${metadata.outlier_count} outliers detected`} 
                  />
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Quality Score" 
                    secondary={
                      <Chip 
                        label={`${(qualityScore * 100).toFixed(0)}%`}
                        color={qualityScore > 0.9 ? 'success' : qualityScore > 0.8 ? 'warning' : 'error'}
                        size="small"
                      />
                    } 
                  />
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Features Table */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Features ({metadata.feature_count})
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Feature Name</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Data Type</TableCell>
                      <TableCell>Description</TableCell>
                      <TableCell align="right">Min</TableCell>
                      <TableCell align="right">Max</TableCell>
                      <TableCell align="right">Mean</TableCell>
                      <TableCell align="right">Null Count</TableCell>
                      <TableCell align="center">Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {metadata.features.map((feature) => (
                      <TableRow key={feature.name} hover>
                        <TableCell>
                          <Typography variant="body2" sx={{ fontWeight: 500 }}>
                            {feature.name}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip 
                            label={feature.feature_type} 
                            color={getFeatureTypeColor(feature.feature_type)}
                            size="small" 
                          />
                        </TableCell>
                        <TableCell>{feature.data_type}</TableCell>
                        <TableCell>
                          <Typography variant="body2" sx={{ maxWidth: 250 }}>
                            {feature.description}
                          </Typography>
                        </TableCell>
                        <TableCell align="right">
                          {feature.min_value?.toFixed(4) || 'N/A'}
                        </TableCell>
                        <TableCell align="right">
                          {feature.max_value?.toFixed(4) || 'N/A'}
                        </TableCell>
                        <TableCell align="right">
                          {feature.mean_value?.toFixed(4) || 'N/A'}
                        </TableCell>
                        <TableCell align="right">{feature.null_count}</TableCell>
                        <TableCell align="center">
                          <Tooltip title="View Distribution">
                            <IconButton size="small" sx={{ color: '#667eea' }}>
                              <ViewIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        {/* Labels Table */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Labels ({metadata.label_count})
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Label Name</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Data Type</TableCell>
                      <TableCell>Description</TableCell>
                      <TableCell align="right">Lead Periods</TableCell>
                      <TableCell align="right">Min</TableCell>
                      <TableCell align="right">Max</TableCell>
                      <TableCell>Distribution</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {metadata.labels.map((label) => (
                      <TableRow key={label.name} hover>
                        <TableCell>
                          <Typography variant="body2" sx={{ fontWeight: 500 }}>
                            {label.name}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip 
                            label={label.label_type} 
                            color="secondary"
                            size="small" 
                          />
                        </TableCell>
                        <TableCell>{label.data_type}</TableCell>
                        <TableCell>
                          <Typography variant="body2" sx={{ maxWidth: 250 }}>
                            {label.description}
                          </Typography>
                        </TableCell>
                        <TableCell align="right">{label.lead_periods}</TableCell>
                        <TableCell align="right">
                          {label.min_value?.toFixed(4) || 'N/A'}
                        </TableCell>
                        <TableCell align="right">
                          {label.max_value?.toFixed(4) || 'N/A'}
                        </TableCell>
                        <TableCell>
                          {label.class_distribution ? (
                            <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                              {Object.entries(label.class_distribution).map(([value, count]) => (
                                <Chip 
                                  key={value} 
                                  label={`${value}: ${count}`} 
                                  size="small" 
                                  variant="outlined" 
                                />
                              ))}
                            </Box>
                          ) : (
                            'Continuous'
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        {/* Generation Parameters */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Generation Parameters
              </Typography>
              <Grid container spacing={2}>
                {Object.entries(metadata.generation_parameters).map(([key, value]) => (
                  <Grid item xs={12} sm={6} md={4} key={key}>
                    <Box>
                      <Typography variant="body2" color="textSecondary">
                        {key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                      </Typography>
                      <Typography variant="body1">
                        {typeof value === 'boolean' ? (value ? 'Yes' : 'No') : String(value)}
                      </Typography>
                    </Box>
                  </Grid>
                ))}
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        {/* File Information */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                File Information
              </Typography>
              <List dense>
                <ListItem>
                  <ListItemText 
                    primary="Features File" 
                    secondary={metadata.feature_file_path} 
                  />
                  <IconButton edge="end">
                    <DownloadIcon />
                  </IconButton>
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Labels File" 
                    secondary={metadata.label_file_path} 
                  />
                  <IconButton edge="end">
                    <DownloadIcon />
                  </IconButton>
                </ListItem>
                <ListItem>
                  <ListItemText 
                    primary="Sample IDs" 
                    secondary={`${metadata.sample_ids.length} samples available`} 
                  />
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default DatasetDetails;