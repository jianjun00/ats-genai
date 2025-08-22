/**
 * DualAxisOHLCChart - Dual-axis OHLC chart implementation
 * 
 * Creates charts with:
 * - Top panel: OHLC candlesticks + Volume bars
 * - Bottom panel: Technical indicators (etop, ebot, pldot, ema)
 * - Synchronized zoom and pan between panels
 */

class DualAxisOHLCChart {
    constructor(containerId, sequenceId) {
        this.containerId = containerId;
        this.sequenceId = sequenceId;
        this.topChart = null;
        this.bottomChart = null;
        this.data = null;
        
        // Chart colors for technical indicators
        this.indicatorColors = {
            'ema': 'rgb(255, 99, 132)',
            'etop': 'rgb(54, 162, 235)', 
            'ebot': 'rgb(255, 205, 86)',
            'pldot': 'rgb(75, 192, 192)',
            'rsi': 'rgb(153, 102, 255)',
            'atr': 'rgb(255, 159, 64)',
            'vwap': 'rgb(199, 199, 199)',
            'macd': 'rgb(83, 102, 255)'
        };
    }
    
    async loadData() {
        try {
            const response = await fetch(`/api/v1/datasets/${currentDatasetId}/sequences/${this.sequenceId}/ohlc`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            this.data = await response.json();
            return this.data;
        } catch (error) {
            console.error('Error loading OHLC data:', error);
            throw error;
        }
    }
    
    async render() {
        // Load data first
        await this.loadData();
        
        // Create chart container structure
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
        await this.renderTopPanel();
        
        // Render bottom panel (Technical Indicators)
        await this.renderBottomPanel();
        
        // Set up synchronization between charts
        this.syncCharts();
    }
    
    async renderTopPanel() {
        const ctx = document.getElementById(`top-chart-${this.sequenceId}`).getContext('2d');
        
        // Process OHLC data
        const ohlcData = this.processOHLCData();
        const volumeData = this.processVolumeData();
        
        const config = {
            type: 'candlestick',
            data: {
                datasets: [
                    {
                        label: `${this.data.symbols.join(', ')} OHLC`,
                        type: 'candlestick',
                        data: ohlcData,
                        yAxisID: 'price-axis',
                        borderColor: {
                            up: 'rgba(26, 152, 129, 1)',
                            down: 'rgba(239, 57, 74, 1)',
                            unchanged: 'rgba(90, 90, 90, 1)'
                        },
                        backgroundColor: {
                            up: 'rgba(26, 152, 129, 0.8)',
                            down: 'rgba(239, 57, 74, 0.8)', 
                            unchanged: 'rgba(90, 90, 90, 0.8)'
                        }
                    },
                    {
                        label: 'Volume',
                        type: 'bar',
                        data: volumeData,
                        yAxisID: 'volume-axis',
                        backgroundColor: volumeData.map((point, index) => {
                            if (index === 0) return 'rgba(54, 162, 235, 0.3)';
                            const prevClose = ohlcData[index - 1]?.c || ohlcData[index]?.o;
                            const currentClose = ohlcData[index]?.c;
                            return currentClose >= prevClose ? 
                                'rgba(26, 152, 129, 0.3)' : 'rgba(239, 57, 74, 0.3)';
                        }),
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
                plugins: {
                    title: {
                        display: true,
                        text: `${this.data.symbols.join(', ')} - Price & Volume`,
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'day',
                            displayFormats: {
                                day: 'MMM DD'
                            }
                        },
                        title: {
                            display: false  // Hide x-axis title on top panel
                        }
                    },
                    'price-axis': {
                        type: 'linear',
                        position: 'left',
                        title: {
                            display: true,
                            text: 'Price ($)',
                            font: { weight: 'bold' }
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    },
                    'volume-axis': {
                        type: 'linear',
                        position: 'right',
                        title: {
                            display: true,
                            text: 'Volume',
                            font: { weight: 'bold' }
                        },
                        grid: {
                            drawOnChartArea: false
                        },
                        ticks: {
                            callback: function(value) {
                                if (value >= 1000000) {
                                    return (value / 1000000).toFixed(1) + 'M';
                                } else if (value >= 1000) {
                                    return (value / 1000).toFixed(1) + 'K';
                                }
                                return value;
                            }
                        }
                    }
                }
            }
        };
        
        this.topChart = new Chart(ctx, config);
    }
    
    async renderBottomPanel() {
        const ctx = document.getElementById(`bottom-chart-${this.sequenceId}`).getContext('2d');
        
        // Process technical indicators data
        const indicatorDatasets = this.processIndicatorData();
        
        if (indicatorDatasets.length === 0) {
            // Show message if no indicators available
            ctx.font = '16px Arial';
            ctx.fillStyle = '#666';
            ctx.textAlign = 'center';
            ctx.fillText('No technical indicators available for this sequence', 
                        ctx.canvas.width / 2, ctx.canvas.height / 2);
            return;
        }
        
        const config = {
            type: 'line',
            data: {
                datasets: indicatorDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Technical Indicators',
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'day',
                            displayFormats: {
                                day: 'MMM DD'
                            }
                        },
                        title: {
                            display: true,
                            text: 'Date',
                            font: { weight: 'bold' }
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Indicator Value',
                            font: { weight: 'bold' }
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    }
                }
            }
        };
        
        this.bottomChart = new Chart(ctx, config);
    }
    
    processOHLCData() {
        if (!this.data.ohlc_data || this.data.ohlc_data.length === 0) {
            return [];
        }
        
        return this.data.ohlc_data
            .filter(point => point.open !== undefined && point.close !== undefined)
            .map(point => ({
                x: new Date(point.date),
                o: point.open,
                h: point.high || Math.max(point.open, point.close),
                l: point.low || Math.min(point.open, point.close),
                c: point.close
            }));
    }
    
    processVolumeData() {
        if (!this.data.volume_data || this.data.volume_data.length === 0) {
            // Try to extract volume from OHLC data
            if (this.data.ohlc_data) {
                return this.data.ohlc_data
                    .filter(point => point.volume !== undefined)
                    .map(point => ({
                        x: new Date(point.date),
                        y: point.volume
                    }));
            }
            return [];
        }
        
        return this.data.volume_data.map(point => ({
            x: new Date(point.date),
            y: point.volume
        }));
    }
    
    processIndicatorData() {
        if (!this.data.technical_indicators) {
            return [];
        }
        
        const datasets = [];
        let colorIndex = 0;
        const defaultColors = [
            'rgb(255, 99, 132)',
            'rgb(54, 162, 235)', 
            'rgb(255, 205, 86)',
            'rgb(75, 192, 192)',
            'rgb(153, 102, 255)',
            'rgb(255, 159, 64)'
        ];
        
        for (const [indicatorName, indicatorData] of Object.entries(this.data.technical_indicators)) {
            if (!indicatorData || indicatorData.length === 0) continue;
            
            const color = this.indicatorColors[indicatorName.toLowerCase()] || 
                         defaultColors[colorIndex % defaultColors.length];
            
            datasets.push({
                label: indicatorName.toUpperCase(),
                data: indicatorData.map(point => ({
                    x: new Date(point.date),
                    y: point.value
                })),
                borderColor: color,
                backgroundColor: color.replace('rgb', 'rgba').replace(')', ', 0.1)'),
                tension: 0.2,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 4,
                borderWidth: 2
            });
            
            colorIndex++;
        }
        
        return datasets;
    }
    
    syncCharts() {
        if (!this.topChart || !this.bottomChart) return;
        
        // Synchronize zoom and pan between charts
        const syncZoom = (sourceChart, targetChart) => {
            const xScale = sourceChart.scales.x;
            if (targetChart.scales.x) {
                targetChart.options.scales.x.min = xScale.min;
                targetChart.options.scales.x.max = xScale.max;
                targetChart.update('none');
            }
        };
        
        // Add zoom event listeners if zoom plugin is available
        if (this.topChart.options.plugins && this.topChart.options.plugins.zoom) {
            this.topChart.options.plugins.zoom.onZoom = () => {
                syncZoom(this.topChart, this.bottomChart);
            };
        }
        
        if (this.bottomChart.options.plugins && this.bottomChart.options.plugins.zoom) {
            this.bottomChart.options.plugins.zoom.onZoom = () => {
                syncZoom(this.bottomChart, this.topChart);
            };
        }
        
        // Sync crosshair/tooltip between charts
        const syncTooltip = (activeChart, targetChart, activeElements) => {
            if (activeElements.length > 0 && targetChart.data.datasets.length > 0) {
                const dataIndex = activeElements[0].index;
                targetChart.tooltip.setActiveElements([{
                    datasetIndex: 0,
                    index: dataIndex
                }], {x: 0, y: 0});
                targetChart.update('none');
            } else {
                targetChart.tooltip.setActiveElements([], {x: 0, y: 0});
                targetChart.update('none');
            }
        };
        
        // Add hover event listeners
        this.topChart.options.onHover = (event, activeElements) => {
            syncTooltip(this.topChart, this.bottomChart, activeElements);
        };
        
        this.bottomChart.options.onHover = (event, activeElements) => {
            syncTooltip(this.bottomChart, this.topChart, activeElements);
        };
    }
    
    destroy() {
        if (this.topChart) {
            this.topChart.destroy();
            this.topChart = null;
        }
        
        if (this.bottomChart) {
            this.bottomChart.destroy();
            this.bottomChart = null;
        }
        
        this.data = null;
    }
    
    // Utility method to update with new data
    async updateData(newSequenceId) {
        this.sequenceId = newSequenceId;
        this.destroy();
        await this.render();
    }
    
    // Method to export chart as image
    exportAsImage(filename = null) {
        if (!this.topChart || !this.bottomChart) return;
        
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        
        // Set canvas size to accommodate both charts
        canvas.width = this.topChart.canvas.width;
        canvas.height = this.topChart.canvas.height + this.bottomChart.canvas.height;
        
        // Draw top chart
        ctx.drawImage(this.topChart.canvas, 0, 0);
        
        // Draw bottom chart below top chart
        ctx.drawImage(this.bottomChart.canvas, 0, this.topChart.canvas.height);
        
        // Download the combined image
        const link = document.createElement('a');
        link.download = filename || `sequence_${this.sequenceId}_ohlc_chart.png`;
        link.href = canvas.toDataURL();
        link.click();
    }
}

// Utility function to format numbers for display
function formatNumber(value, decimals = 2) {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    
    if (Math.abs(value) >= 1000000) {
        return (value / 1000000).toFixed(decimals) + 'M';
    } else if (Math.abs(value) >= 1000) {
        return (value / 1000).toFixed(decimals) + 'K';
    } else {
        return Number(value).toFixed(decimals);
    }
}

// Utility function to format dates for display
function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        year: 'numeric'
    });
}

// Export the class for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DualAxisOHLCChart;
}