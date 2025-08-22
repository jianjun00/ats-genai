import React, { useState, useEffect } from 'react';
import CurrentPortfolioDashboard from './components/CurrentPortfolioDashboard';
import ComparisonDashboard from './components/ComparisonDashboard';
import './App.css';

const App = () => {
  const [activeTab, setActiveTab] = useState('current');
  const [connectionStatus, setConnectionStatus] = useState('connecting');

  useEffect(() => {
    // Test backend connection
    fetch('http://localhost:8001/api/v1/portfolio/status')
      .then(response => {
        if (response.ok) {
          setConnectionStatus('connected');
        } else {
          setConnectionStatus('error');
        }
      })
      .catch(() => {
        setConnectionStatus('error');
      });
  }, []);

  return (
    <div className="portfolio-app">
      {/* Header */}
      <div className="app-header">
        <h1>Portfolio Manager</h1>
        <div className="header-controls">
          <div className={`connection-status ${connectionStatus}`}>
            <span className="status-indicator"></span>
            {connectionStatus === 'connected' ? 'Live' : 
             connectionStatus === 'error' ? 'Offline' : 'Connecting...'}
          </div>
          <div className="last-update">
            Last update: {new Date().toLocaleTimeString()}
          </div>
        </div>
      </div>

      {/* Navigation */}
      <div className="app-nav">
        <button 
          className={`nav-tab ${activeTab === 'current' ? 'active' : ''}`}
          onClick={() => setActiveTab('current')}
        >
          Current Portfolio
        </button>
        <button 
          className={`nav-tab ${activeTab === 'comparison' ? 'active' : ''}`}
          onClick={() => setActiveTab('comparison')}
        >
          Strategy Comparison
        </button>
      </div>

      {/* Main Content */}
      <div className="app-main">
        {activeTab === 'current' && <CurrentPortfolioDashboard />}
        {activeTab === 'comparison' && <ComparisonDashboard />}
      </div>

      {/* Footer */}
      <div className="app-footer">
        <div className="footer-content">
          <span>Portfolio Manager v1.0</span>
          <span>Real-time market data</span>
        </div>
      </div>
    </div>
  );
};

export default App;