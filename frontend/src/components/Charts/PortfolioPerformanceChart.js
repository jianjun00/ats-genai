import React, { useState, useEffect, useCallback, useMemo } from 'react';
import Plot from 'react-plotly.js';
import { Button, Select, Space } from 'antd';
import { ZoomInOutlined, UndoOutlined } from '@ant-design/icons';
import moment from 'moment';

const { Option } = Select;

const PortfolioPerformanceChart = ({
  data,
  realTimeData,
  benchmarkData,
  chartType = 'performance',
  onDrillDown,
  loading
}) => {
  const [selectedRange, setSelectedRange] = useState(null);
  const [zoomLevel, setZoomLevel] = useState('auto');
  const [chartMode, setChartMode] = useState('lines');
  
  // Combine historical and real-time data
  const chartData = useMemo(() => {
    if (!data) return null;
    
    let combined = [...data];
    if (realTimeData?.latestPerformance) {
      // Add or update the latest data point
      const latestDate = realTimeData.latestPerformance.date;
      const existingIndex = combined.findIndex(d => d.date === latestDate);
      
      if (existingIndex >= 0) {
        combined[existingIndex] = realTimeData.latestPerformance;
      } else {
        combined.push(realTimeData.latestPerformance);
      }
    }
    
    return combined;
  }, [data, realTimeData]);
  
  // Prepare chart data based on chart type
  const traces = useMemo(() => {
    if (!chartData) return [];
    
    const traces = [];
    
    if (chartType === 'performance') {
      // Main portfolio performance line
      traces.push({
        x: chartData.map(d => d.date),
        y: chartData.map(d => d.portfolio_value),
        type: 'scatter',
        mode: chartMode,
        name: 'Portfolio Value',
        line: {
          color: '#1f77b4',
          width: 2
        },
        hovertemplate: '<b>%{fullData.name}</b><br>' +
          'Date: %{x}<br>' +
          'Value: $%{y:,.2f}<br>' +
          '<extra></extra>'
      });
      
      // Benchmark comparison if available
      if (benchmarkData) {
        traces.push({
          x: benchmarkData.map(d => d.date),
          y: benchmarkData.map(d => d.portfolio_value),
          type: 'scatter',
          mode: 'lines',
          name: 'Benchmark',
          line: {
            color: '#ff7f0e',
            width: 2,
            dash: 'dash'
          },
          hovertemplate: '<b>%{fullData.name}</b><br>' +
            'Date: %{x}<br>' +
            'Value: $%{y:,.2f}<br>' +
            '<extra></extra>'
        });
      }
      
      // Add real-time indicator
      if (realTimeData?.isConnected && realTimeData?.latestPerformance) {
        const latest = realTimeData.latestPerformance;
        traces.push({
          x: [latest.date],
          y: [latest.portfolio_value],
          type: 'scatter',
          mode: 'markers',
          name: 'Live Data',
          marker: {
            color: '#2ca02c',
            size: 8,
            symbol: 'circle'
          },
          hovertemplate: '<b>Live Data</b><br>' +
            'Date: %{x}<br>' +
            'Value: $%{y:,.2f}<br>' +
            '<extra></extra>'
        });
      }
    } else if (chartType === 'rolling') {
      // Rolling metrics chart
      traces.push({
        x: chartData.map(d => d.date),
        y: chartData.map(d => d.rolling_sharpe || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Rolling Sharpe Ratio',
        yaxis: 'y',
        line: {
          color: '#2ca02c',
          width: 2
        }
      });
      
      traces.push({
        x: chartData.map(d => d.date),
        y: chartData.map(d => d.rolling_volatility || 0),
        type: 'scatter',
        mode: 'lines',
        name: 'Rolling Volatility',
        yaxis: 'y2',
        line: {
          color: '#d62728',
          width: 2
        }
      });
    }
    
    return traces;
  }, [chartData, benchmarkData, chartType, chartMode, realTimeData]);
  
  // Chart layout configuration
  const layout = useMemo(() => {
    const baseLayout = {
      title: {
        text: chartType === 'performance' ? 'Portfolio Performance Over Time' : 'Rolling Performance Metrics',
        font: { size: 16, weight: 600 }
      },
      xaxis: {
        title: 'Date',
        type: 'date',
        showgrid: true,
        gridcolor: '#f0f0f0'
      },
      yaxis: {
        title: chartType === 'performance' ? 'Portfolio Value ($)' : 'Sharpe Ratio',
        showgrid: true,
        gridcolor: '#f0f0f0',
        tickformat: chartType === 'performance' ? '$,.0f' : '.2f'
      },
      hovermode: 'x unified',
      showlegend: true,
      legend: {
        orientation: 'h',
        y: -0.2
      },
      margin: {
        l: 60,
        r: 40,
        t: 60,
        b: 100
      },
      plot_bgcolor: 'white',
      paper_bgcolor: 'white'
    };
    
    // Add second y-axis for rolling chart
    if (chartType === 'rolling') {
      baseLayout.yaxis2 = {
        title: 'Volatility',
        overlaying: 'y',
        side: 'right',
        showgrid: false,
        tickformat: '.2%'
      };
    }
    
    return baseLayout;
  }, [chartType]);
  
  // Chart configuration
  const config = {
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false,
    responsive: true
  };
  
  // Handle chart selection for drill-down
  const handleSelection = useCallback((eventData) => {
    if (eventData && eventData.range) {
      const { x: [startDate, endDate] } = eventData.range;
      
      setSelectedRange({ startDate, endDate });
      
      // Trigger drill-down analysis
      onDrillDown({
        type: 'time_period',
        period: {
          start: moment(startDate).format('YYYY-MM-DD'),
          end: moment(endDate).format('YYYY-MM-DD')
        },
        chartType: 'portfolio_performance',
        data: chartData.filter(d => {
          const date = moment(d.date);
          return date.isBetween(startDate, endDate, 'day', '[]');
        })
      });
    }
  }, [chartData, onDrillDown]);
  
  // Handle chart clicks
  const handleClick = useCallback((eventData) => {
    if (eventData.points && eventData.points.length > 0) {
      const point = eventData.points[0];
      const clickedDate = point.x;
      
      // Trigger drill-down for specific date
      onDrillDown({
        type: 'single_date',
        date: moment(clickedDate).format('YYYY-MM-DD'),
        chartType: 'portfolio_performance',
        value: point.y,
        data: chartData.find(d => d.date === clickedDate)
      });
    }
  }, [chartData, onDrillDown]);
  
  // Reset zoom
  const handleResetZoom = useCallback(() => {
    setSelectedRange(null);
    setZoomLevel('auto');
  }, []);
  
  if (loading) {
    return (
      <div className="loading-container">
        <div>Loading chart...</div>
      </div>
    );
  }
  
  if (!chartData || chartData.length === 0) {
    return (
      <div className="error-container">
        <div>No data available for chart</div>
      </div>
    );
  }
  
  return (
    <div className="portfolio-chart-container">
      {/* Chart Controls */}
      <div className="chart-controls" style={{ marginBottom: 16 }}>
        <Space>
          <Select
            value={chartMode}
            onChange={setChartMode}
            style={{ width: 120 }}
          >
            <Option value="lines">Lines</Option>
            <Option value="lines+markers">Lines + Markers</Option>
            <Option value="markers">Markers Only</Option>
          </Select>
          
          <Button
            icon={<UndoOutlined />}
            onClick={handleResetZoom}
            disabled={!selectedRange}
          >
            Reset Zoom
          </Button>
          
          {realTimeData?.isConnected && (
            <div className="real-time-indicator">
              <div className="real-time-dot" />
              <span>Live Updates</span>
            </div>
          )}
        </Space>
      </div>
      
      {/* Main Chart */}
      <Plot
        data={traces}
        layout={layout}
        config={config}
        onSelected={handleSelection}
        onClick={handleClick}
        style={{ width: '100%', height: '400px' }}
        useResizeHandler={true}
      />
      
      {/* Selection Info */}
      {selectedRange && (
        <div className="selected-period-info">
          <strong>Selected Period:</strong> {' '}
          {moment(selectedRange.startDate).format('MMM DD, YYYY')} - {' '}
          {moment(selectedRange.endDate).format('MMM DD, YYYY')}
          <Button
            type="link"
            size="small"
            onClick={() => setSelectedRange(null)}
            style={{ marginLeft: 8 }}
          >
            Clear Selection
          </Button>
        </div>
      )}
    </div>
  );
};

export default PortfolioPerformanceChart;