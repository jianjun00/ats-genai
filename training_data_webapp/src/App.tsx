import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { Box } from '@mui/material';

import Sidebar from './components/Sidebar';
import TrainingDataTable from './components/TrainingDataTable';
import DatasetDetails from './components/DatasetDetails';
import FeatureDistributions from './components/FeatureDistributions';
import DatasetComparison from './components/DatasetComparison';
import TrainingSequenceViewer from './components/TrainingSequenceViewer';

const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#667eea',
    },
    secondary: {
      main: '#764ba2',
    },
    background: {
      default: '#0f0f23',
      paper: '#1a1a2e',
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h4: {
      fontWeight: 600,
    },
    h6: {
      fontWeight: 500,
    },
  },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundImage: 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%)',
          backdropFilter: 'blur(10px)',
          border: '1px solid rgba(255,255,255,0.1)',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%)',
          backdropFilter: 'blur(10px)',
        },
      },
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
          <Sidebar />
          <Box 
            component="main" 
            sx={{ 
              flexGrow: 1, 
              p: 3,
              background: 'linear-gradient(135deg, #0f0f23 0%, #1a1a2e 100%)',
              minHeight: '100vh'
            }}
          >
            <Routes>
              <Route path="/" element={<TrainingDataTable />} />
              <Route path="/datasets/:datasetId" element={<DatasetDetails />} />
              <Route path="/distributions/:datasetId" element={<FeatureDistributions />} />
              <Route path="/compare" element={<DatasetComparison />} />
              <Route path="/sequence/:datasetId/:sequenceId" element={<TrainingSequenceViewer />} />
            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;