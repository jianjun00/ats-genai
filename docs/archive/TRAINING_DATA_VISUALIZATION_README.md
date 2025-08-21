# 📊 Training Data Visualization System

A comprehensive web application for visualizing, exploring, and analyzing machine learning training data with metadata tracking, feature distributions, and interactive charts.

## 🌟 Features

### 📋 **Training Data Table**
- **Interactive Dataset Browser**: View all training datasets in a searchable, sortable table
- **Quality Metrics**: Dataset quality scores, completeness ratios, and data health indicators
- **Quick Actions**: One-click access to distributions, details, and sequence viewers
- **Multi-Symbol Support**: Filter and search by stock symbols, cryptocurrencies, or other assets

### 🔍 **Dataset Details**
- **Comprehensive Metadata**: Creation timestamps, sequence counts, feature/label information
- **Feature Analysis**: Detailed breakdown of all features with types, descriptions, and statistics
- **Label Information**: Target variable details including classification distributions
- **Data Quality Dashboard**: Missing data ratios, outlier counts, and generation parameters

### 📈 **Feature Distributions**
- **Interactive Histograms**: Visualize feature value distributions with zoom and pan
- **Time Series Charts**: View how features evolve over time
- **OHLC Candlestick Charts**: Special visualization for Open, High, Low, Close data
- **Price Indicator Charts**: Technical indicators with proper scaling and context
- **Statistical Summaries**: Mean, std dev, percentiles, and distribution metrics

### 🔬 **Training Sequence Viewer**
- **Individual Sequence Analysis**: Drill down into specific training examples
- **Multi-View Visualization**: Time series, heatmaps, predictions, and data tables
- **Feature Context**: See how features change over the sequence length
- **Label Predictions**: Visualize target predictions over the prediction horizon
- **Interactive Navigation**: Browse through thousands of sequences with ease

### ⚖️ **Dataset Comparison**
- **Side-by-Side Analysis**: Compare two datasets across all dimensions
- **Statistical Testing**: Kolmogorov-Smirnov and T-tests for distribution differences
- **Visual Comparisons**: Overlaid distribution charts and difference metrics
- **Quality Comparison**: Compare data quality, completeness, and other metrics
- **Symbol Overlap Analysis**: Identify common and unique symbols between datasets

### 🎯 **Metadata System**
- **Feature Type Classification**: int, float, OHLC, price_indicator, volume_indicator, return, classification, binary, normalized
- **Primary Key Tracking**: Identify which feature serves as the unique identifier
- **Visualization Hints**: Automatic chart type selection based on feature types
- **File Path Tracking**: Complete lineage from raw data to processed features
- **Generation Parameters**: Full audit trail of how data was created

## 🏗️ Architecture

### 🖥️ **Frontend (React + TypeScript)**
```
training_data_webapp/
├── src/
│   ├── components/
│   │   ├── TrainingDataTable.tsx      # Main dataset browser
│   │   ├── DatasetDetails.tsx         # Detailed metadata view
│   │   ├── FeatureDistributions.tsx   # Distribution visualizations
│   │   ├── TrainingSequenceViewer.tsx # Individual sequence analysis
│   │   ├── DatasetComparison.tsx      # Multi-dataset comparison
│   │   └── Sidebar.tsx               # Navigation and quick actions
│   ├── types/
│   │   └── TrainingData.ts           # TypeScript type definitions
│   └── App.tsx                       # Main application component
├── package.json                      # Dependencies and scripts
└── public/                          # Static assets
```

### ⚙️ **Backend (Python Flask)**
```
training_data_api.py                 # REST API server
src/modeling/
├── training_data_metadata.py        # Metadata management system
└── configurable_train_data_generator.py  # Enhanced with metadata generation
```

### 🗄️ **Data Storage**
```
training_data_output/
├── dataset_YYYYMMDD_HHMMSS_features.npy    # Feature arrays
├── dataset_YYYYMMDD_HHMMSS_labels.npy      # Label arrays
├── dataset_YYYYMMDD_HHMMSS_metadata.json   # Comprehensive metadata
├── dataset_YYYYMMDD_HHMMSS_names.json      # Feature/label names
└── dataset_YYYYMMDD_HHMMSS_masks.npy       # Data validity masks
```

## 🚀 Quick Start

### 1. **Generate Training Data with Metadata**
```python
from src.modeling.configurable_train_data_generator import ConfigurableTrainingDataGenerator
from src.modeling.training_data_metadata import TrainingDataMetadataManager
import gin

# Configure the generator
gin.parse_config_file('config/configurable_training_simple.gin')
config = create_configurable_training_data_config()

# Initialize generator with output directory
generator = ConfigurableTrainingDataGenerator(config, output_dir="training_data_output")

# Generate training data (automatically creates metadata)
result = generator.generate_training_data(your_market_data, symbols=['AAPL', 'MSFT'])

# Metadata and files are automatically saved
print(f"Dataset ID: {result['dataset_id']}")
print(f"Metadata file: {result['metadata_file']}")
print(f"Sample IDs: {len(result['sample_ids'])}")
```

### 2. **Start the API Server**
```bash
# Install dependencies
pip install flask flask-cors numpy pandas scipy

# Start the server
python training_data_api.py
```

### 3. **Launch the Web Application**
```bash
# Navigate to the webapp directory
cd training_data_webapp

# Install dependencies
npm install

# Start the development server
npm start
```

### 4. **Access the Application**
- **Web Interface**: http://localhost:3000
- **API Documentation**: http://localhost:5000/api/v1/health
- **Dataset Browser**: http://localhost:3000/
- **Distributions**: http://localhost:3000/distributions/{dataset_id}
- **Sequence Viewer**: http://localhost:3000/sequence/{dataset_id}/{sequence_id}

## 📊 Metadata Schema

### **Feature Metadata**
```typescript
interface FeatureMetadata {
  name: string;                    // Feature name
  feature_type: FeatureType;       // int, float, ohlc, price_indicator, etc.
  data_type: string;              // numpy dtype (float64, int32, etc.)
  dimension: number;              // Feature dimension (1 for scalar)
  description: string;            // Human-readable description
  source_column?: string;         // Original data column
  lag_periods?: number;           // For lagged features
  window_size?: number;           // For windowed features
  visualization_type: VisualizationType;  // Suggested chart type
  min_value?: number;             // Minimum value in dataset
  max_value?: number;             // Maximum value in dataset
  mean_value?: number;            // Mean value
  std_value?: number;             // Standard deviation
  null_count: number;             // Number of missing values
  is_primary_key: boolean;        // Can be used as unique identifier
}
```

### **Dataset Metadata**
```typescript
interface TrainingDataMetadata {
  dataset_name: string;           // Unique dataset identifier
  creation_timestamp: string;     // ISO timestamp
  total_sequences: number;        // Number of training sequences
  sequence_length: number;        // Input sequence length
  prediction_horizon: number;     // Prediction time steps
  feature_count: number;          // Number of features
  label_count: number;           // Number of labels
  features: FeatureMetadata[];    // Detailed feature information
  labels: LabelMetadata[];        // Detailed label information
  symbols: string[];              // Asset symbols included
  date_range: {                   // Data date coverage
    start: string;
    end: string;
  };
  data_file_path: string;         // Path to main data file
  sample_ids: string[];           // Unique IDs for each sequence
  primary_key_feature: string;    // Primary key feature name
  generation_parameters: Record<string, any>;  // How data was generated
  data_quality_metrics: Record<string, number>; // Quality assessments
}
```

## 🎨 Visualization Types

### **Feature Type → Chart Mapping**
- **OHLC**: Candlestick charts with volume
- **Price Indicators**: Line charts with proper scaling
- **Volume Indicators**: Bar charts and time series
- **Returns**: Distribution histograms with outlier detection
- **Classifications**: Bar charts with class distributions
- **Binary**: Simple bar charts (0/1 counts)
- **Normalized**: Distribution charts with standardized scales

### **Interactive Features**
- **Zoom & Pan**: All charts support interactive exploration
- **Tooltips**: Hover for detailed value information
- **Export**: Download charts as PNG/SVG
- **Responsive**: Adapts to different screen sizes
- **Dark Theme**: Professional dark mode interface

## 🔧 Configuration

### **Feature Types**
```python
class FeatureType(Enum):
    INT = "int"                          # Integer values
    FLOAT = "float"                      # Floating point values
    OHLC = "ohlc"                       # Open, High, Low, Close data
    PRICE_INDICATOR = "price_indicator"  # Price-based technical indicators
    VOLUME_INDICATOR = "volume_indicator" # Volume-based indicators
    RETURN = "return"                    # Return calculations
    CLASSIFICATION = "classification"     # Categorical/class labels
    BINARY = "binary"                    # Binary 0/1 features
    NORMALIZED = "normalized"            # Normalized/scaled features
```

### **API Endpoints**
```bash
GET /api/v1/datasets                    # List all datasets
GET /api/v1/datasets/{id}              # Get dataset metadata
GET /api/v1/datasets/{id}/distributions # Get feature distributions
GET /api/v1/datasets/{id}/sequences/{seq_id} # Get training sequence
GET /api/v1/compare?dataset1={id1}&dataset2={id2} # Compare datasets
GET /api/v1/health                     # Health check
```

## 📈 Use Cases

### **Model Development**
- **Feature Engineering**: Understand feature distributions and quality
- **Data Validation**: Ensure training data meets quality standards
- **Debugging**: Identify problematic sequences or features
- **Comparison**: Compare different preprocessing approaches

### **Research & Analysis**
- **Distribution Analysis**: Study how features behave across different market regimes
- **Quality Assessment**: Measure data completeness and identify gaps
- **Temporal Analysis**: Understand how features evolve over time
- **Cross-Asset Comparison**: Compare feature behavior across different symbols

### **Production Monitoring**
- **Data Drift Detection**: Compare new data against historical baselines
- **Quality Monitoring**: Track data quality metrics over time
- **Feature Importance**: Understand which features contribute most to model performance
- **Audit Trail**: Maintain complete lineage from raw data to model inputs

## 🛠️ Development

### **Adding New Feature Types**
```python
# 1. Add to FeatureType enum
class FeatureType(Enum):
    CUSTOM_TYPE = "custom_type"

# 2. Update visualization mapping
viz_type_map = {
    FeatureType.CUSTOM_TYPE: VisualizationType.CUSTOM_CHART
}

# 3. Add chart component in React
const renderCustomChart = (data) => {
    // Custom visualization logic
};
```

### **Extending Metadata**
```python
# Add custom fields to metadata
@dataclass
class FeatureMetadata:
    # ... existing fields ...
    custom_metric: Optional[float] = None
    tags: List[str] = field(default_factory=list)
```

### **Custom Visualizations**
```typescript
// Add new chart types in React
const CustomChart: React.FC<{data: any}> = ({ data }) => {
    return (
        <Plot
            data={[{
                // Custom Plotly configuration
            }]}
            layout={{
                // Custom layout
            }}
        />
    );
};
```

## 🔐 Security & Performance

### **Data Security**
- No sensitive data exposed through API
- File access restricted to designated directories
- Input validation on all endpoints
- CORS properly configured for frontend access

### **Performance Optimizations**
- **Lazy Loading**: Large datasets loaded on demand
- **Caching**: Metadata cached in memory
- **Streaming**: Large arrays served in chunks
- **Pagination**: Table results paginated for responsiveness

### **Scalability**
- **Database Backend**: Easy migration to PostgreSQL/TimescaleDB
- **Cloud Storage**: S3/GCS support for large datasets
- **Distributed Processing**: Ray/Dask integration ready
- **API Rate Limiting**: Built-in protection against abuse

## 📚 Examples

### **Generate Sample Data**
```python
# Create sample training data
result = test_simple_configurable.py  # Run the working example
print(f"Generated dataset: {result['dataset_id']}")
print(f"Features shape: {result['features'].shape}")
print(f"Labels shape: {result['labels'].shape}")
```

### **View in Browser**
1. Start API: `python training_data_api.py`
2. Start webapp: `cd training_data_webapp && npm start`
3. Open: http://localhost:3000
4. Explore your datasets with rich visualizations!

---

## ✨ **Ready to Explore Your Training Data!**

This comprehensive system provides everything needed to understand, validate, and optimize machine learning training datasets. From individual sequence analysis to cross-dataset comparisons, every aspect of your training data is now visual and accessible.

🚀 **Get started by generating your first dataset with metadata and exploring it in the interactive web interface!**