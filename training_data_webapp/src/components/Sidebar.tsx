import React from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Typography,
  Box,
  Divider,
  Chip
} from '@mui/material';
import {
  TableView as TableIcon,
  Analytics as AnalyticsIcon,
  CompareArrows as CompareIcon,
  Timeline as TimelineIcon,
  Dataset as DatasetIcon,
  TrendingUp as TrendingUpIcon
} from '@mui/icons-material';

const drawerWidth = 280;

const menuItems = [
  {
    path: '/',
    label: 'Training Data Table',
    icon: <TableIcon />,
    description: 'Browse and explore all training datasets'
  },
  {
    path: '/compare',
    label: 'Dataset Comparison',
    icon: <CompareIcon />,
    description: 'Compare features between datasets'
  }
];

const Sidebar: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: drawerWidth,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: drawerWidth,
          boxSizing: 'border-box',
          background: 'linear-gradient(180deg, #1a1a2e 0%, #16213e 100%)',
          borderRight: '1px solid rgba(255,255,255,0.1)',
        },
      }}
    >
      <Box sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <DatasetIcon sx={{ color: '#667eea', mr: 1, fontSize: 28 }} />
          <Typography variant="h6" sx={{ 
            color: 'white', 
            fontWeight: 600,
            background: 'linear-gradient(45deg, #667eea, #764ba2)',
            backgroundClip: 'text',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent'
          }}>
            Training Data Explorer
          </Typography>
        </Box>
        <Chip 
          label="v1.0.0" 
          size="small" 
          sx={{ 
            backgroundColor: 'rgba(102, 126, 234, 0.2)',
            color: '#667eea',
            fontSize: '0.7rem'
          }} 
        />
      </Box>
      
      <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)' }} />
      
      <List sx={{ px: 2 }}>
        {menuItems.map((item) => (
          <ListItem key={item.path} disablePadding sx={{ mb: 1 }}>
            <ListItemButton
              selected={location.pathname === item.path}
              onClick={() => navigate(item.path)}
              sx={{
                borderRadius: 2,
                '&.Mui-selected': {
                  backgroundColor: 'rgba(102, 126, 234, 0.2)',
                  '&:hover': {
                    backgroundColor: 'rgba(102, 126, 234, 0.3)',
                  },
                },
                '&:hover': {
                  backgroundColor: 'rgba(255,255,255,0.05)',
                },
              }}
            >
              <ListItemIcon sx={{ 
                color: location.pathname === item.path ? '#667eea' : 'rgba(255,255,255,0.7)',
                minWidth: 40 
              }}>
                {item.icon}
              </ListItemIcon>
              <ListItemText 
                primary={
                  <Typography variant="body2" sx={{ 
                    color: location.pathname === item.path ? 'white' : 'rgba(255,255,255,0.8)',
                    fontWeight: location.pathname === item.path ? 600 : 400
                  }}>
                    {item.label}
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" sx={{ 
                    color: 'rgba(255,255,255,0.5)',
                    fontSize: '0.7rem',
                    mt: 0.5
                  }}>
                    {item.description}
                  </Typography>
                }
              />
            </ListItemButton>
          </ListItem>
        ))}
      </List>

      <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)', mx: 2, my: 2 }} />

      <Box sx={{ px: 3, py: 2 }}>
        <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.5)', mb: 1, display: 'block' }}>
          Quick Actions
        </Typography>
        
        <List dense>
          <ListItem disablePadding>
            <ListItemButton 
              sx={{ 
                borderRadius: 1, 
                py: 0.5,
                '&:hover': { backgroundColor: 'rgba(255,255,255,0.05)' }
              }}
            >
              <ListItemIcon sx={{ minWidth: 30 }}>
                <AnalyticsIcon sx={{ fontSize: 16, color: 'rgba(255,255,255,0.6)' }} />
              </ListItemIcon>
              <ListItemText 
                primary={
                  <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.7)' }}>
                    Feature Analysis
                  </Typography>
                } 
              />
            </ListItemButton>
          </ListItem>
          
          <ListItem disablePadding>
            <ListItemButton 
              sx={{ 
                borderRadius: 1, 
                py: 0.5,
                '&:hover': { backgroundColor: 'rgba(255,255,255,0.05)' }
              }}
            >
              <ListItemIcon sx={{ minWidth: 30 }}>
                <TrendingUpIcon sx={{ fontSize: 16, color: 'rgba(255,255,255,0.6)' }} />
              </ListItemIcon>
              <ListItemText 
                primary={
                  <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.7)' }}>
                    Quality Metrics
                  </Typography>
                } 
              />
            </ListItemButton>
          </ListItem>
        </List>
      </Box>
    </Drawer>
  );
};

export default Sidebar;