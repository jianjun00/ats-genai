import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';

const InteractiveTrainingDataTable = ({ dataset }) => {
  const [tableData, setTableData] = useState([]);
  const [selectedRowIndex, setSelectedRowIndex] = useState(null);
  const [chartData, setChartData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';

  // Generate sample table data based on dataset metadata
  const generateSampleTableData = useCallback(() => {
    if (!dataset) return [];
    
    const sampleData = [];
    const totalRows = dataset.total_sequences * dataset.sequence_length;
    
    // Show first 50 rows for table view
    for (let i = 0; i < Math.min(50, totalRows); i++) {
      const sequenceIdx = Math.floor(i / dataset.sequence_length);
      const timeStep = i % dataset.sequence_length;
      
      sampleData.push({
        rowIndex: i,
        sequenceIdx: sequenceIdx,
        timeStep: timeStep,
        description: `Sequence ${sequenceIdx}, Step ${timeStep}`,
        symbol: dataset.symbols[0] || 'UNKNOWN',
        indicators: dataset.technical_indicators || 'OHLCV + Technical'
      });
    }
    
    return sampleData;
  }, [dataset]);

  // Load table data
  useEffect(() => {
    if (dataset) {
      setTableData(generateSampleTableData());
    }
  }, [dataset, generateSampleTableData]);

  // Fetch OHLC data for visualization when row is selected
  const fetchChartData = async (rowIndex) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(
        `${API_BASE_URL}/api/v1/training-datasets/${dataset.id}/visualization-data?start_idx=${rowIndex}&count=21`
      );
      
      if (!response.ok) throw new Error('Failed to fetch chart data');
      
      const data = await response.json();
      setChartData(data);
      
    } catch (err) {
      setError(`Failed to load chart data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Handle row selection
  const handleRowSelect = (rowIndex) => {
    setSelectedRowIndex(rowIndex);
    fetchChartData(rowIndex);
  };

  // Create Plotly OHLC chart with technical indicators
  const createPlotlyChart = () => {
    if (!chartData || !chartData.data) return null;

    const data = chartData.data;
    
    // OHLC Candlestick trace
    const ohlcTrace = {
      x: data.map((_, i) => i),
      open: data.map(d => d.open || 0),
      high: data.map(d => d.high || 0),
      low: data.map(d => d.low || 0),
      close: data.map(d => d.close || 0),
      type: 'candlestick',
      name: 'OHLC',
      yaxis: 'y',
      increasing: { line: { color: '#26C281' } },
      decreasing: { line: { color: '#ED5565' } },
      marker: {
        line: { width: 1 }
      }
    };

    // Technical Indicators
    const traces = [ohlcTrace];

    // Envelope Top (etop)
    if (data.some(d => d.etop !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.etop || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Envelope Top',
        line: { color: '#FF6B6B', width: 2 },
        yaxis: 'y'
      });
    }

    // Envelope Bottom (ebot)
    if (data.some(d => d.ebot !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.ebot || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Envelope Bottom',
        line: { color: '#4ECDC4', width: 2 },
        yaxis: 'y'
      });
    }

    // PLDot indicator
    if (data.some(d => d.pldot !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.pldot || 0),
        type: 'scatter',
        mode: 'markers+lines',
        name: 'PLDot',
        line: { color: '#FFA726', width: 1 },
        marker: { size: 4 },
        yaxis: 'y2'
      });
    }

    // Z1B indicator
    if (data.some(d => d.z1b !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.z1b || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Z1B',
        line: { color: '#AB47BC', width: 1, dash: 'dash' },
        yaxis: 'y2'
      });
    }

    // Z2B indicator
    if (data.some(d => d.z2b !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.z2b || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Z2B',
        line: { color: '#7E57C2', width: 1, dash: 'dot' },
        yaxis: 'y2'
      });
    }

    // Z5T indicator  
    if (data.some(d => d.z5t !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.z5t || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Z5T',
        line: { color: '#66BB6A', width: 1 },
        yaxis: 'y2'
      });
    }

    // Z6T indicator
    if (data.some(d => d.z6t !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.z6t || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Z6T',
        line: { color: '#42A5F5', width: 1 },
        yaxis: 'y2'
      });
    }

    // Volume trace (on separate axis)
    if (data.some(d => d.volume !== undefined)) {
      traces.push({
        x: data.map((_, i) => i),
        y: data.map(d => d.volume || 0),
        type: 'bar',
        name: 'Volume',
        marker: { 
          color: data.map(d => d.close > d.open ? 'rgba(38, 194, 129, 0.3)' : 'rgba(237, 85, 101, 0.3)'),
          line: { width: 0 }
        },
        yaxis: 'y3'
      });
    }

    // Add vertical line for selected point
    const selectedIndex = data.findIndex(d => d.is_selected);
    if (selectedIndex >= 0) {
      traces.push({
        x: [selectedIndex, selectedIndex],
        y: [Math.min(...data.map(d => d.low || 0)), Math.max(...data.map(d => d.high || 0))],
        type: 'scatter',
        mode: 'lines',
        name: 'Selected',
        line: { color: '#FF5722', width: 3, dash: 'dash' },
        showlegend: false,
        yaxis: 'y'
      });
    }

    const layout = {
      title: {
        text: `Training Data Visualization - Sequence ${chartData.sequence_idx}, Step ${chartData.selected_time_step}`,
        font: { size: 16 }
      },
      xaxis: {
        title: 'Time Steps',
        showgrid: true,
        rangeslider: { visible: false }
      },
      yaxis: {
        title: 'Price',
        domain: [0.4, 1],
        showgrid: true
      },
      yaxis2: {
        title: 'Indicators',
        domain: [0.2, 0.35],
        showgrid: false
      },
      yaxis3: {
        title: 'Volume',
        domain: [0, 0.15],
        showgrid: false
      },
      height: 600,
      showlegend: true,
      legend: {
        x: 1.02,
        y: 1,
        bgcolor: 'rgba(255, 255, 255, 0.8)'
      },
      margin: { r: 150 },
      hovermode: 'x unified'
    };

    return { data: traces, layout };
  };

  return (
    <div className="interactive-training-data-table">
      <div className="table-chart-container" style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
        
        {/* Plotly Chart */}
        <div className="chart-section">
          <h4>📊 OHLC Chart with Technical Indicators</h4>
          {loading && <div className="loading">Loading chart data...</div>}
          {error && <div className="error" style={{ color: 'red' }}>Error: {error}</div>}
          
          {chartData && !loading && (
            <Plot
              data={createPlotlyChart()?.data || []}
              layout={createPlotlyChart()?.layout || {}}
              config={{
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d']
              }}
              style={{ width: '100%', height: '600px' }}
            />
          )}
          
          {!chartData && !loading && selectedRowIndex === null && (
            <div className="no-selection" style={{ 
              textAlign: 'center', 
              padding: '40px', 
              background: '#f8f9fa', 
              borderRadius: '8px',
              border: '2px dashed #dee2e6'
            }}>
              <p>📈 Select a row from the table below to view OHLC chart with technical indicators</p>
              <p>Chart will show 10 bars before and 10 bars after the selected point</p>
            </div>
          )}
        </div>

        {/* Data Table with Selection */}
        <div className="table-section">
          <h4>📋 Training Dataset Rows</h4>
          <p>Click on any row to view the OHLC chart and technical indicators for that time period</p>
          
          <div className="table-container" style={{ maxHeight: '400px', overflowY: 'auto' }}>
            <table className="training-data-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#f8f9fa' }}>
                <tr>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Row Index</th>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Sequence</th>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Time Step</th>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Description</th>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Symbol</th>
                  <th style={{ border: '1px solid #ddd', padding: '12px' }}>Indicators</th>
                </tr>
              </thead>
              <tbody>
                {tableData.map((row) => (
                  <tr
                    key={row.rowIndex}
                    onClick={() => handleRowSelect(row.rowIndex)}
                    style={{
                      cursor: 'pointer',
                      backgroundColor: selectedRowIndex === row.rowIndex ? '#e3f2fd' : 'transparent',
                      ':hover': { backgroundColor: '#f5f5f5' }
                    }}
                    onMouseEnter={(e) => {
                      if (selectedRowIndex !== row.rowIndex) {
                        e.target.style.backgroundColor = '#f5f5f5';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (selectedRowIndex !== row.rowIndex) {
                        e.target.style.backgroundColor = 'transparent';
                      }
                    }}
                  >
                    <td style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'center' }}>
                      {row.rowIndex}
                    </td>
                    <td style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'center' }}>
                      {row.sequenceIdx}
                    </td>
                    <td style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'center' }}>
                      {row.timeStep}
                    </td>
                    <td style={{ border: '1px solid #ddd', padding: '8px' }}>
                      {row.description}
                    </td>
                    <td style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'center' }}>
                      <strong>{row.symbol}</strong>
                    </td>
                    <td style={{ border: '1px solid #ddd', padding: '8px', fontSize: '0.9em' }}>
                      {row.indicators}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {dataset && (
            <div className="dataset-info" style={{ marginTop: '15px', fontSize: '0.9em', color: '#666' }}>
              <p>
                📊 Total: {dataset.total_sequences?.toLocaleString()} sequences × {dataset.sequence_length} steps = {(dataset.total_sequences * dataset.sequence_length)?.toLocaleString()} total rows
              </p>
              <p>
                🔧 Features: {dataset.feature_count} | 🎯 Labels: {dataset.label_count}
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default InteractiveTrainingDataTable;