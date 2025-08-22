import React, { useState, useEffect, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Row, Col, Card, DatePicker, Select, Button, Space, message } from 'antd';
import { ReloadOutlined, DownloadOutlined } from '@ant-design/icons';
import moment from 'moment';

import MetricsSummary from './MetricsSummary';
import PortfolioPerformanceChart from '../Charts/PortfolioPerformanceChart';
import DrawdownChart from '../Charts/DrawdownChart';
import AttributionChart from '../Charts/AttributionChart';
import ModelPerformanceChart from '../Charts/ModelPerformanceChart';
import ForecastVisualization from '../Charts/ForecastVisualization';
import DrillDownPanel from '../DrillDown/DrillDownPanel';
import { usePortfolioData, useRealTimeUpdates } from '../../hooks/usePortfolioData';
import { exportToPDF, exportToExcel } from '../../utils/exportUtils';

const { RangePicker } = DatePicker;
const { Option } = Select;

const Dashboard = () => {
  const { backtestId } = useParams();
  const [timeRange, setTimeRange] = useState([
    moment().subtract(6, 'months'),
    moment()
  ]);
  const [selectedMetrics, setSelectedMetrics] = useState(['portfolio', 'attribution', 'model']);
  const [benchmarkId, setBenchmarkId] = useState(null);
  const [drillDownData, setDrillDownData] = useState(null);
  const [isDrillDownOpen, setIsDrillDownOpen] = useState(false);
  const [enableRealTime, setEnableRealTime] = useState(false);
  
  // Custom hooks for data management
  const {
    portfolioData,
    attributionData,
    modelData,
    loading,
    error,
    refetch
  } = usePortfolioData(backtestId, timeRange, benchmarkId);
  
  const realTimeData = useRealTimeUpdates(backtestId, enableRealTime);
  
  // Handle time range changes
  const handleTimeRangeChange = useCallback((dates) => {
    setTimeRange(dates);
  }, []);
  
  // Handle drill-down interactions
  const handleDrillDown = useCallback((drillDownInfo) => {
    setDrillDownData(drillDownInfo);
    setIsDrillDownOpen(true);
  }, []);
  
  const closeDrillDown = useCallback(() => {
    setIsDrillDownOpen(false);
    setDrillDownData(null);
  }, []);
  
  // Handle data refresh
  const handleRefresh = useCallback(async () => {
    try {
      await refetch();
      message.success('Data refreshed successfully');
    } catch (err) {
      message.error('Failed to refresh data');
    }
  }, [refetch]);
  
  // Handle export functions
  const handleExportPDF = useCallback(async () => {
    try {
      await exportToPDF(backtestId, {
        portfolioData,
        attributionData,
        modelData,
        timeRange
      });
      message.success('PDF exported successfully');
    } catch (err) {
      message.error('Failed to export PDF');
    }
  }, [backtestId, portfolioData, attributionData, modelData, timeRange]);
  
  const handleExportExcel = useCallback(async () => {
    try {
      await exportToExcel(backtestId, {
        portfolioData,
        attributionData,
        modelData,
        timeRange
      });
      message.success('Excel exported successfully');
    } catch (err) {
      message.error('Failed to export Excel');
    }
  }, [backtestId, portfolioData, attributionData, modelData, timeRange]);
  
  // Error handling
  if (error) {
    return (
      <div className="error-container">
        <div className="error-message">
          Failed to load dashboard data: {error.message}
        </div>
        <Button onClick={handleRefresh} type="primary">
          Retry
        </Button>
      </div>
    );
  }
  
  return (
    <div className="dashboard-container">
      {/* Filter Panel */}
      <Card className="filter-panel" size="small">
        <div className="filter-row">
          <div className="filter-item">
            <div className="filter-label">Time Range</div>
            <RangePicker
              value={timeRange}
              onChange={handleTimeRangeChange}
              format="YYYY-MM-DD"
              allowClear={false}
            />
          </div>
          
          <div className="filter-item">
            <div className="filter-label">Benchmark</div>
            <Select
              value={benchmarkId}
              onChange={setBenchmarkId}
              placeholder="Select benchmark"
              allowClear
              style={{ width: 180 }}
            >
              <Option value="spy_etf">SPY ETF</Option>
              <Option value="static_2023_100">Static Strategy</Option>
              <Option value="buy_hold">Buy & Hold</Option>
            </Select>
          </div>
          
          <div className="filter-item">
            <div className="filter-label">Display Metrics</div>
            <Select
              mode="multiple"
              value={selectedMetrics}
              onChange={setSelectedMetrics}
              placeholder="Select metrics"
              style={{ width: 200 }}
            >
              <Option value="portfolio">Portfolio Performance</Option>
              <Option value="attribution">Attribution Analysis</Option>
              <Option value="model">Model Performance</Option>
              <Option value="forecasts">Forecast Accuracy</Option>
            </Select>
          </div>
          
          <div className="filter-item">
            <Space>
              <Button
                icon={<ReloadOutlined />}
                onClick={handleRefresh}
                loading={loading}
              >
                Refresh
              </Button>
              
              <Button
                icon={<DownloadOutlined />}
                onClick={handleExportPDF}
              >
                Export PDF
              </Button>
              
              <Button
                icon={<DownloadOutlined />}
                onClick={handleExportExcel}
              >
                Export Excel
              </Button>
            </Space>
          </div>
        </div>
      </Card>
      
      {/* Metrics Summary */}
      <MetricsSummary
        data={portfolioData?.metrics}
        realTimeData={realTimeData}
        loading={loading}
        onDrillDown={handleDrillDown}
      />
      
      {/* Main Charts Grid */}
      <Row gutter={[24, 24]}>
        {/* Portfolio Performance Chart */}
        {selectedMetrics.includes('portfolio') && (
          <Col span={24}>
            <Card 
              title="Portfolio Performance" 
              className="chart-container"
              extra={
                realTimeData?.isConnected && (
                  <div className="real-time-indicator">
                    <div className="real-time-dot" />
                    Live Data
                  </div>
                )
              }
            >
              <PortfolioPerformanceChart
                data={portfolioData?.performance}
                realTimeData={realTimeData}
                benchmarkData={portfolioData?.benchmark}
                onDrillDown={handleDrillDown}
                loading={loading}
              />
            </Card>
          </Col>
        )}
        
        {/* Risk Analysis Charts */}
        <Col span={12}>
          <Card title="Drawdown Analysis" className="chart-container">
            <DrawdownChart
              data={portfolioData?.drawdown}
              onDrillDown={handleDrillDown}
              loading={loading}
            />
          </Card>
        </Col>
        
        <Col span={12}>
          <Card title="Rolling Metrics" className="chart-container">
            <div style={{ height: 300 }}>
              {/* Rolling Sharpe Ratio and Volatility Chart */}
              <PortfolioPerformanceChart
                data={portfolioData?.rollingMetrics}
                chartType="rolling"
                onDrillDown={handleDrillDown}
                loading={loading}
              />
            </div>
          </Card>
        </Col>
        
        {/* Attribution Analysis */}
        {selectedMetrics.includes('attribution') && (
          <Col span={24}>
            <Card title="Performance Attribution" className="chart-container">
              <AttributionChart
                data={attributionData}
                onDrillDown={handleDrillDown}
                loading={loading}
              />
            </Card>
          </Col>
        )}
        
        {/* Model Performance */}
        {selectedMetrics.includes('model') && (
          <>
            <Col span={12}>
              <Card title="Model Accuracy Over Time" className="chart-container">
                <ModelPerformanceChart
                  data={modelData?.accuracy}
                  chartType="accuracy"
                  onDrillDown={handleDrillDown}
                  loading={loading}
                />
              </Card>
            </Col>
            
            <Col span={12}>
              <Card title="Confidence Calibration" className="chart-container">
                <ModelPerformanceChart
                  data={modelData?.confidence}
                  chartType="confidence"
                  onDrillDown={handleDrillDown}
                  loading={loading}
                />
              </Card>
            </Col>
          </>
        )}
        
        {/* Forecast Visualization */}
        {selectedMetrics.includes('forecasts') && (
          <Col span={24}>
            <Card title="Support/Resistance Forecasts" className="chart-container">
              <ForecastVisualization
                data={modelData?.forecasts}
                onDrillDown={handleDrillDown}
                loading={loading}
              />
            </Card>
          </Col>
        )}
      </Row>
      
      {/* Drill-Down Panel */}
      <DrillDownPanel
        isOpen={isDrillDownOpen}
        onClose={closeDrillDown}
        data={drillDownData}
        backtestId={backtestId}
      />
    </div>
  );
};

export default Dashboard;