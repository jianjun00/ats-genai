# Dual-Axis OHLC Chart Architecture

## Overview

This document outlines the architecture for implementing dual-axis OHLC charts with:
- **Top Panel**: OHLC candlesticks + Volume bars
- **Bottom Panel**: Technical indicators (etop, ebot, pldot, ema)
- **Individual sequence-level charts** for dataset detail pages

## Chart.js Implementation Strategy

### 1. Dual-Panel Layout Structure

```html
<!-- Container for dual-axis chart -->
<div class="dual-chart-container">
    <div class="chart-panel top-panel">
        <canvas id="ohlc-volume-chart-{sequence_id}"></canvas>
    </div>
    <div class="chart-panel bottom-panel">
        <canvas id="indicators-chart-{sequence_id}"></canvas>
    </div>
</div>
```

### 2. Top Panel: OHLC + Volume Chart

**Chart Type**: Mixed chart with dual Y-axes
- **Primary Y-axis (left)**: Price scale for OHLC
- **Secondary Y-axis (right)**: Volume scale

```javascript
const topPanelConfig = {
    type: 'candlestick',  // Using Chart.js chartjs-chart-financial plugin
    data: {
        datasets: [
            {
                label: 'OHLC',
                type: 'candlestick',
                data: ohlcData.map(point => ({
                    x: point.date,
                    o: point.open,
                    h: point.high,  
                    l: point.low,
                    c: point.close
                })),
                yAxisID: 'price-axis'
            },
            {
                label: 'Volume',
                type: 'bar',
                data: volumeData.map(point => ({
                    x: point.date,
                    y: point.volume
                })),
                yAxisID: 'volume-axis',
                backgroundColor: 'rgba(54, 162, 235, 0.3)',
                borderColor: 'rgba(54, 162, 235, 0.8)',
                borderWidth: 1
            }
        ]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            intersect: false,
            mode: 'index'
        },
        scales: {
            x: {
                type: 'time',
                time: {
                    unit: 'day',
                    displayFormats: {
                        day: 'MMM DD'
                    }
                }
            },
            'price-axis': {
                type: 'linear',
                position: 'left',
                title: {
                    display: true,
                    text: 'Price ($)'
                }
            },
            'volume-axis': {
                type: 'linear',
                position: 'right',
                title: {
                    display: true,
                    text: 'Volume'
                },
                grid: {
                    drawOnChartArea: false
                }
            }
        }
    }
};
```

### 3. Bottom Panel: Technical Indicators

**Chart Type**: Line chart with multiple indicators
- **Single Y-axis**: Normalized indicator scale
- **Multiple lines**: etop, ebot, pldot, ema

```javascript
const bottomPanelConfig = {
    type: 'line',
    data: {
        datasets: [
            {
                label: 'EMA',
                data: indicatorData.ema,
                borderColor: 'rgb(255, 99, 132)',
                backgroundColor: 'rgba(255, 99, 132, 0.1)',
                tension: 0.1
            },
            {
                label: 'ETOP',
                data: indicatorData.etop,
                borderColor: 'rgb(54, 162, 235)', 
                backgroundColor: 'rgba(54, 162, 235, 0.1)',
                tension: 0.1
            },
            {
                label: 'EBOT',
                data: indicatorData.ebot,
                borderColor: 'rgb(255, 205, 86)',
                backgroundColor: 'rgba(255, 205, 86, 0.1)', 
                tension: 0.1
            },
            {
                label: 'PLDOT',
                data: indicatorData.pldot,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.1)',
                tension: 0.1
            }
        ]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            x: {
                type: 'time',
                time: {
                    unit: 'day',
                    displayFormats: {
                        day: 'MMM DD'
                    }
                }
            },
            y: {
                title: {
                    display: true,
                    text: 'Indicator Value'
                }
            }
        },
        plugins: {
            legend: {
                display: true,
                position: 'top'
            }
        }
    }
};
```

## CSS Styling for Dual-Panel Layout

```css
.dual-chart-container {
    width: 100%;
    height: 600px;
    display: flex;
    flex-direction: column;
    border: 1px solid #ddd;
    border-radius: 8px;
    overflow: hidden;
}

.chart-panel {
    position: relative;
    width: 100%;
}

.top-panel {
    height: 60%; /* 60% for OHLC + Volume */
    border-bottom: 2px solid #eee;
    background: linear-gradient(to bottom, #fafafa, #ffffff);
}

.bottom-panel {
    height: 40%; /* 40% for technical indicators */
    background: linear-gradient(to bottom, #ffffff, #f8f9fa);
}

.chart-panel canvas {
    width: 100% !important;
    height: 100% !important;
}

/* Responsive design */
@media (max-width: 768px) {
    .dual-chart-container {
        height: 500px;
    }
    
    .top-panel {
        height: 65%;
    }
    
    .bottom-panel {
        height: 35%;
    }
}
```

## JavaScript Implementation

```javascript
class DualAxisOHLCChart {
    constructor(containerId, sequenceId) {
        this.containerId = containerId;
        this.sequenceId = sequenceId;
        this.topChart = null;
        this.bottomChart = null;
    }
    
    async loadData() {
        const response = await fetch(`/api/v1/datasets/${datasetId}/sequences/${this.sequenceId}/ohlc`);
        return await response.json();
    }
    
    async render() {
        const data = await this.loadData();
        
        // Create chart containers
        const container = document.getElementById(this.containerId);
        container.innerHTML = `
            <div class="dual-chart-container">
                <div class="chart-panel top-panel">
                    <canvas id="top-chart-${this.sequenceId}"></canvas>
                </div>
                <div class="chart-panel bottom-panel">
                    <canvas id="bottom-chart-${this.sequenceId}"></canvas>
                </div>
            </div>
        `;
        
        // Render top panel (OHLC + Volume)
        const topCtx = document.getElementById(`top-chart-${this.sequenceId}`).getContext('2d');
        this.topChart = new Chart(topCtx, this.buildTopPanelConfig(data));
        
        // Render bottom panel (Technical Indicators)
        const bottomCtx = document.getElementById(`bottom-chart-${this.sequenceId}`).getContext('2d');
        this.bottomChart = new Chart(bottomCtx, this.buildBottomPanelConfig(data));
        
        // Sync zoom and pan between charts
        this.syncCharts();
    }
    
    buildTopPanelConfig(data) {
        return {
            type: 'candlestick',
            data: {
                datasets: [
                    {
                        label: `${data.symbols.join(', ')} OHLC`,
                        type: 'candlestick',
                        data: data.ohlc_data,
                        yAxisID: 'price-axis'
                    },
                    {
                        label: 'Volume',
                        type: 'bar',
                        data: data.volume_data,
                        yAxisID: 'volume-axis',
                        backgroundColor: 'rgba(54, 162, 235, 0.3)'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        time: { unit: 'day' }
                    },
                    'price-axis': {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'Price ($)' }
                    },
                    'volume-axis': {
                        type: 'linear', 
                        position: 'right',
                        title: { display: true, text: 'Volume' },
                        grid: { drawOnChartArea: false }
                    }
                }
            }
        };
    }
    
    buildBottomPanelConfig(data) {
        const indicators = data.technical_indicators;
        const datasets = [];
        const colors = ['rgb(255, 99, 132)', 'rgb(54, 162, 235)', 'rgb(255, 205, 86)', 'rgb(75, 192, 192)'];
        
        let colorIndex = 0;
        for (const [indicatorName, indicatorData] of Object.entries(indicators)) {
            datasets.push({
                label: indicatorName.toUpperCase(),
                data: indicatorData,
                borderColor: colors[colorIndex % colors.length],
                backgroundColor: colors[colorIndex % colors.length].replace('rgb', 'rgba').replace(')', ', 0.1)'),
                tension: 0.1
            });
            colorIndex++;
        }
        
        return {
            type: 'line',
            data: { datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        time: { unit: 'day' }
                    },
                    y: {
                        title: { display: true, text: 'Indicator Value' }
                    }
                },
                plugins: {
                    legend: { display: true, position: 'top' }
                }
            }
        };
    }
    
    syncCharts() {
        // Synchronize zoom and pan between top and bottom charts
        const syncZoom = (sourceChart, targetChart) => {
            const xScale = sourceChart.scales.x;
            targetChart.options.scales.x.min = xScale.min;
            targetChart.options.scales.x.max = xScale.max;
            targetChart.update('none');
        };
        
        this.topChart.options.onZoom = () => syncZoom(this.topChart, this.bottomChart);
        this.bottomChart.options.onZoom = () => syncZoom(this.bottomChart, this.topChart);
    }
    
    destroy() {
        if (this.topChart) this.topChart.destroy();
        if (this.bottomChart) this.bottomChart.destroy();
    }
}
```

## Integration with Dataset Detail Page

### 1. Sequence Row OHLC Links

```html
<tr>
    <td>{sequence.sequence_id}</td>
    <td>{sequence.start_date}</td>
    <td>{sequence.symbols.join(', ')}</td>
    <td>
        <button class="btn-chart" onclick="showSequenceOHLC({sequence.sequence_id}, '{sequence.symbols.join(', ')}')">
            📈 OHLC Chart
        </button>
    </td>
</tr>
```

### 2. Modal System for Chart Display

```html
<div id="sequence-ohlc-modal" class="modal">
    <div class="modal-content large-modal">
        <div class="modal-header">
            <h2 class="modal-title">Sequence OHLC Chart</h2>
            <span class="close" onclick="closeModal('sequence-ohlc-modal')">&times;</span>
        </div>
        <div class="modal-body">
            <div id="sequence-ohlc-content"></div>
        </div>
    </div>
</div>
```

### 3. JavaScript Function for Sequence OHLC

```javascript
async function showSequenceOHLC(sequenceId, symbols) {
    const modal = document.getElementById('sequence-ohlc-modal');
    const content = document.getElementById('sequence-ohlc-content');
    const title = modal.querySelector('.modal-title');
    
    // Update modal title
    title.textContent = `Sequence ${sequenceId} - ${symbols} OHLC & Indicators`;
    
    // Show modal with loading
    modal.style.display = 'block';
    content.innerHTML = '<div class="loading-spinner">📊 Loading OHLC chart...</div>';
    
    try {
        // Create and render dual-axis chart
        const chart = new DualAxisOHLCChart('sequence-ohlc-content', sequenceId);
        await chart.render();
        
        // Store chart reference for cleanup
        window.currentSequenceChart = chart;
        
    } catch (error) {
        content.innerHTML = `<div style="color: red;">❌ Error loading OHLC chart: ${error.message}</div>`;
    }
}

// Cleanup when modal closes
function closeModal(modalId) {
    document.getElementById(modalId).style.display = 'none';
    
    if (modalId === 'sequence-ohlc-modal' && window.currentSequenceChart) {
        window.currentSequenceChart.destroy();
        window.currentSequenceChart = null;
    }
}
```

## Dependencies Required

### 1. Chart.js Plugins
```html
<!-- Core Chart.js -->
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<!-- Chart.js Financial plugin for candlestick charts -->
<script src="https://cdn.jsdelivr.net/npm/chartjs-chart-financial"></script>

<!-- Chart.js Date adapter for time scales -->
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>

<!-- Chart.js Zoom plugin for pan/zoom functionality -->
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom"></script>
```

### 2. Data Processing Utilities

```javascript
class ChartDataProcessor {
    static processOHLCData(rawData) {
        return rawData.ohlc_data.map(point => ({
            x: new Date(point.date),
            o: point.open,
            h: point.high,
            l: point.low,
            c: point.close
        }));
    }
    
    static processVolumeData(rawData) {
        return rawData.volume_data.map(point => ({
            x: new Date(point.date),
            y: point.volume
        }));
    }
    
    static processIndicatorData(rawData) {
        const processed = {};
        for (const [name, data] of Object.entries(rawData.technical_indicators)) {
            processed[name] = data.map(point => ({
                x: new Date(point.date),
                y: point.value
            }));
        }
        return processed;
    }
}
```

## Performance Optimizations

### 1. Chart Reuse Strategy
- Maintain chart instances in a cache
- Reuse existing charts when switching between sequences
- Implement proper cleanup to prevent memory leaks

### 2. Data Caching
- Cache sequence OHLC data for recently viewed sequences
- Implement LRU cache with size limits
- Prefetch OHLC data for sequences in current view

### 3. Responsive Loading
- Progressive data loading for large sequences
- Implement data decimation for performance
- Lazy load technical indicators that aren't immediately visible

## Testing Strategy

### 1. Unit Tests
- Test data processing functions
- Test chart configuration generation
- Test dual-panel synchronization

### 2. Integration Tests
- Test sequence OHLC endpoint with real data
- Test chart rendering with various indicator combinations
- Test responsive behavior across screen sizes

### 3. Performance Tests
- Test chart rendering with large sequence datasets
- Test memory usage with multiple open charts
- Test responsiveness during pan/zoom operations

This architecture provides a comprehensive foundation for implementing the dual-axis OHLC charts with the exact specifications requested: price/volume on top, technical indicators on bottom, with individual sequence-level access.