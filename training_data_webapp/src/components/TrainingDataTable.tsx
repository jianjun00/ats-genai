import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Chip,
  IconButton,
  Tooltip,
  TextField,
  InputAdornment,
  Grid,
  LinearProgress,
  Alert,
  Button,
  Menu,
  MenuItem
} from '@mui/material';
import {
  Visibility as ViewIcon,
  Analytics as AnalyticsIcon,
  Compare as CompareIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  Download as DownloadIcon,
  MoreVert as MoreIcon
} from '@mui/icons-material';
import { DatasetSummary } from '../types/TrainingData';

const TrainingDataTable: React.FC = () => {
  const navigate = useNavigate();
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null);

  // Mock data - in real app, this would come from an API
  const mockDatasets: DatasetSummary[] = [
    {
      id: 'dataset_20250820_143022',
      name: 'AAPL/MSFT 2-Symbol Training Set',
      creation_timestamp: '2025-08-20T14:30:22Z',
      total_sequences: 66,
      feature_count: 5,
      label_count: 2,
      symbols: ['AAPL', 'MSFT'],
      date_range: {
        start: '2025-06-30',
        end: '2025-08-18'
      },
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
      date_range: {
        start: '2024-01-01',
        end: '2025-08-19'
      },
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
      date_range: {
        start: '2023-01-01',
        end: '2025-08-18'
      },
      quality_score: 0.82,
      size_mb: 18.3
    }
  ];

  useEffect(() => {
    // Simulate API call
    const loadDatasets = async () => {
      try {
        setLoading(true);
        // In real app: const response = await api.getDatasets();
        await new Promise(resolve => setTimeout(resolve, 1000)); // Simulate delay
        setDatasets(mockDatasets);
      } catch (err) {
        setError('Failed to load datasets');
      } finally {
        setLoading(false);
      }
    };

    loadDatasets();
  }, []);

  const filteredDatasets = datasets.filter(dataset =>
    dataset.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    dataset.symbols.some(symbol => symbol.toLowerCase().includes(searchTerm.toLowerCase()))
  );

  const paginatedDatasets = filteredDatasets.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  );

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleViewDataset = (datasetId: string) => {
    navigate(`/datasets/${datasetId}`);
  };

  const handleViewDistributions = (datasetId: string) => {
    navigate(`/distributions/${datasetId}`);
  };

  const handleCompareDatasets = () => {
    if (selectedDatasets.length >= 2) {
      navigate(`/compare?datasets=${selectedDatasets.join(',')}`);
    }
  };

  const handleMenuClick = (event: React.MouseEvent<HTMLButtonElement>, datasetId: string) => {
    setAnchorEl(event.currentTarget);
    setSelectedDatasetId(datasetId);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedDatasetId(null);
  };

  const getQualityColor = (score: number) => {
    if (score >= 0.9) return 'success';
    if (score >= 0.8) return 'warning';
    return 'error';
  };

  const formatFileSize = (sizeInMB: number) => {
    if (sizeInMB < 1) return `${(sizeInMB * 1024).toFixed(0)} KB`;
    if (sizeInMB < 1024) return `${sizeInMB.toFixed(1)} MB`;
    return `${(sizeInMB / 1024).toFixed(1)} GB`;
  };

  const formatDate = (isoString: string) => {
    return new Date(isoString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  if (loading) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Training Data Explorer
        </Typography>
        <Card>
          <CardContent>
            <LinearProgress />
            <Typography sx={{ mt: 2, textAlign: 'center' }}>
              Loading training datasets...
            </Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (error) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
          Training Data Explorer
        </Typography>
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom sx={{ color: 'white', mb: 3 }}>
        Training Data Explorer
      </Typography>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="body2">
                Total Datasets
              </Typography>
              <Typography variant="h4" sx={{ color: '#667eea' }}>
                {datasets.length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="body2">
                Total Sequences
              </Typography>
              <Typography variant="h4" sx={{ color: '#667eea' }}>
                {datasets.reduce((sum, d) => sum + d.total_sequences, 0).toLocaleString()}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="body2">
                Unique Symbols
              </Typography>
              <Typography variant="h4" sx={{ color: '#667eea' }}>
                {new Set(datasets.flatMap(d => d.symbols)).size}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom variant="body2">
                Total Size
              </Typography>
              <Typography variant="h4" sx={{ color: '#667eea' }}>
                {formatFileSize(datasets.reduce((sum, d) => sum + d.size_mb, 0))}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Search and Actions */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', flexWrap: 'wrap' }}>
            <TextField
              placeholder="Search datasets or symbols..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon />
                  </InputAdornment>
                ),
              }}
              sx={{ minWidth: 300 }}
            />
            <Button
              variant="outlined"
              startIcon={<CompareIcon />}
              onClick={handleCompareDatasets}
              disabled={selectedDatasets.length < 2}
            >
              Compare Selected ({selectedDatasets.length})
            </Button>
            <Button
              variant="outlined"
              startIcon={<FilterIcon />}
            >
              Filters
            </Button>
          </Box>
        </CardContent>
      </Card>

      {/* Main Table */}
      <Card>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Dataset</TableCell>
                <TableCell>Symbols</TableCell>
                <TableCell align="right">Sequences</TableCell>
                <TableCell align="right">Features</TableCell>
                <TableCell align="right">Labels</TableCell>
                <TableCell>Date Range</TableCell>
                <TableCell>Quality</TableCell>
                <TableCell align="right">Size</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {paginatedDatasets.map((dataset) => (
                <TableRow 
                  key={dataset.id} 
                  hover
                  sx={{ 
                    '&:hover': { 
                      backgroundColor: 'rgba(102, 126, 234, 0.05)' 
                    }
                  }}
                >
                  <TableCell>
                    <Box>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        {dataset.name}
                      </Typography>
                      <Typography variant="caption" color="textSecondary">
                        {formatDate(dataset.creation_timestamp)}
                      </Typography>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {dataset.symbols.map(symbol => (
                        <Chip 
                          key={symbol} 
                          label={symbol} 
                          size="small" 
                          variant="outlined"
                          sx={{ fontSize: '0.7rem' }}
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell align="right">
                    {dataset.total_sequences.toLocaleString()}
                  </TableCell>
                  <TableCell align="right">
                    {dataset.feature_count}
                  </TableCell>
                  <TableCell align="right">
                    {dataset.label_count}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {dataset.date_range.start} to {dataset.date_range.end}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={`${(dataset.quality_score * 100).toFixed(0)}%`}
                      color={getQualityColor(dataset.quality_score)}
                      size="small"
                    />
                  </TableCell>
                  <TableCell align="right">
                    {formatFileSize(dataset.size_mb)}
                  </TableCell>
                  <TableCell align="center">
                    <Box sx={{ display: 'flex', gap: 0.5 }}>
                      <Tooltip title="View Details">
                        <IconButton 
                          size="small" 
                          onClick={() => handleViewDataset(dataset.id)}
                          sx={{ color: '#667eea' }}
                        >
                          <ViewIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Feature Distributions">
                        <IconButton 
                          size="small" 
                          onClick={() => handleViewDistributions(dataset.id)}
                          sx={{ color: '#667eea' }}
                        >
                          <AnalyticsIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="More Options">
                        <IconButton 
                          size="small"
                          onClick={(e) => handleMenuClick(e, dataset.id)}
                        >
                          <MoreIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        
        <TablePagination
          component="div"
          count={filteredDatasets.length}
          page={page}
          onPageChange={handleChangePage}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          rowsPerPageOptions={[5, 10, 25, 50]}
        />
      </Card>

      {/* Context Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
      >
        <MenuItem onClick={() => {
          if (selectedDatasetId) handleViewDataset(selectedDatasetId);
          handleMenuClose();
        }}>
          <ViewIcon sx={{ mr: 1 }} fontSize="small" />
          View Details
        </MenuItem>
        <MenuItem onClick={() => {
          if (selectedDatasetId) handleViewDistributions(selectedDatasetId);
          handleMenuClose();
        }}>
          <AnalyticsIcon sx={{ mr: 1 }} fontSize="small" />
          View Distributions
        </MenuItem>
        <MenuItem onClick={handleMenuClose}>
          <DownloadIcon sx={{ mr: 1 }} fontSize="small" />
          Download Dataset
        </MenuItem>
      </Menu>
    </Box>
  );
};

export default TrainingDataTable;