import React, { useState } from 'react';
import './App.css';

import SimpleAnalyticsDashboard from './components/SimpleAnalyticsDashboard';
import EnhancedAnalyticsDashboard from './components/EnhancedAnalyticsDashboard';

function App() {
  const [useEnhanced, setUseEnhanced] = useState(true);

  return (
    <div className="App">
      <div style={{ 
        position: 'fixed', 
        top: '10px', 
        right: '10px', 
        zIndex: 1000,
        background: 'rgba(26, 35, 50, 0.9)',
        padding: '0.5rem 1rem',
        borderRadius: '8px',
        border: '1px solid rgba(0, 212, 255, 0.3)'
      }}>
        <label style={{ color: '#ffffff', fontSize: '0.875rem' }}>
          <input 
            type="checkbox" 
            checked={useEnhanced} 
            onChange={(e) => setUseEnhanced(e.target.checked)}
            style={{ marginRight: '0.5rem' }}
          />
          Enhanced Dashboard
        </label>
      </div>
      
      {useEnhanced ? (
        <EnhancedAnalyticsDashboard />
      ) : (
        <SimpleAnalyticsDashboard />
      )}
    </div>
  );
}

export default App;