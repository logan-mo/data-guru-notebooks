# Data Guru Notebooks

A collection of data processing and analysis projects demonstrating various big data technologies and performance optimization techniques. This repository contains two main projects showcasing different approaches to data processing, from time-series data fetching with multiprocessing to performance comparisons across multiple DataFrame libraries.

## Repository Structure

```
data-guru-notebooks/
├── README.md
├── project_01/
│   └── data_gurus_project_01.ipynb
└── project_02/
    ├── pandas_log_balancing_cell_voltages.ipynb
    ├── polars_log_balancing_cell_voltages.ipynb
    └── spark_log_balancing_cell_voltages.ipynb
```

## Projects Overview

### Project 01: InfluxDB Data Pipeline with Multiprocessing

**File:** `project_01/data_gurus_project_01.ipynb`

A comprehensive data pipeline that demonstrates efficient data fetching from InfluxDB using multiprocessing for performance optimization. This project showcases enterprise-level data processing techniques with proper authentication, logging, and data persistence.

#### Key Features:
- **Multiprocessed Data Fetching**: Utilizes Python's multiprocessing library to fetch data concurrently from InfluxDB servers
- **Authentication & Security**: Implements secure authentication with InfluxDB using masked credentials
- **Data Preprocessing**: Comprehensive data cleaning and preparation workflows
- **Delta Table Storage**: Saves processed data as Delta Tables for efficient querying and storage
- **Comprehensive Logging**: Custom logging configuration with proper log management

#### Workflow:
1. **Initialization**: Set up Spark session and logging configuration
2. **Data Preprocessing**: Load and prepare initial datasets
3. **Authentication**: Secure connection to InfluxDB with credentials
4. **Data Fetching**: Multiprocessed queries to InfluxDB for optimal performance
5. **Post Processing**: Data transformation and cleaning
6. **Persistence**: Save final results as Delta Tables

#### Technologies Used:
- **Apache Spark (PySpark)**: Distributed data processing
- **InfluxDB**: Time-series database for data storage
- **Pandas**: Data manipulation and analysis
- **Multiprocessing**: Concurrent data fetching
- **Delta Tables**: Efficient data storage format
- **Python Logging**: Comprehensive logging system

### Project 02: Log Parsing Performance Comparison

**Files:** 
- `project_02/pandas_log_balancing_cell_voltages.ipynb`
- `project_02/polars_log_balancing_cell_voltages.ipynb`
- `project_02/spark_log_balancing_cell_voltages.ipynb`

A performance comparison study implementing identical log parsing functionality across three different DataFrame libraries: Pandas, Polars, and Apache Spark. This project demonstrates how different technologies handle the same data processing task and provides insights into their relative performance characteristics.

#### Key Features:
- **Multi-Framework Implementation**: Same functionality implemented in Pandas, Polars, and Spark
- **Performance Benchmarking**: Timing measurements for each implementation
- **Log Parsing**: Extract cell voltages, balancing status, and metrics from log files
- **Data Persistence**: Save processed data as Parquet files
- **Error Handling**: Robust handling of existing files with append/create logic

#### Data Processing Pipeline:
1. **Data Loading**: Read CSV log files (`log_balancing_cell_voltages copy.csv`)
2. **Log Parsing**: Extract structured data from unstructured log lines
   - Cell voltage extraction
   - Balancing status identification
   - Metrics parsing
3. **Data Transformation**: Clean and structure the extracted data
4. **Performance Measurement**: Time each processing step
5. **Data Persistence**: Save results as Parquet files with conflict resolution

#### Framework Implementations:

##### Pandas Implementation
- **File**: `pandas_log_balancing_cell_voltages.ipynb`
- **Strengths**: Familiar API, extensive ecosystem
- **Use Case**: Small to medium datasets, exploratory analysis

##### Polars Implementation
- **File**: `polars_log_balancing_cell_voltages.ipynb`
- **Strengths**: High performance, memory efficiency
- **Use Case**: Fast processing of medium to large datasets

##### Spark Implementation
- **File**: `spark_log_balancing_cell_voltages.ipynb`
- **Strengths**: Distributed processing, scalability
- **Use Case**: Large-scale data processing, cluster environments

## Technology Stack

### Core Technologies
- **Apache Spark (PySpark)**: Distributed data processing framework
- **Pandas**: Data manipulation and analysis library
- **Polars**: High-performance DataFrame library
- **InfluxDB**: Time-series database
- **NumPy**: Numerical computing library

### Data Storage & Formats
- **Delta Tables**: ACID-compliant data storage
- **Parquet**: Columnar storage format
- **CSV**: Input data format

### Development & Operations
- **Python Logging**: Comprehensive logging system
- **Multiprocessing**: Concurrent processing capabilities
- **Jupyter Notebooks**: Interactive development environment

## Setup Instructions

### Prerequisites
- Python 3.7+
- Java 8+ (for Spark)
- Access to InfluxDB instance (for Project 01)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd data-guru-notebooks
   ```

2. **Install required dependencies:**
   ```bash
   pip install pyspark pandas polars numpy influxdb urllib3 jupyter
   ```

3. **For Spark setup, ensure Java is installed:**
   ```bash
   java -version
   ```

4. **Start Jupyter Notebook:**
   ```bash
   jupyter notebook
   ```

### Configuration

#### Project 01 Configuration
- Update InfluxDB connection parameters in the notebook
- Set appropriate authentication credentials
- Modify data preprocessing parameters as needed

#### Project 02 Configuration
- Ensure the input CSV file `log_balancing_cell_voltages copy.csv` is available
- Adjust output file paths if necessary

## Usage Examples

### Running Project 01
```python
# Initialize Spark session
spark = SparkSession.builder.appName('Joining Data').getOrCreate()

# Set up InfluxDB authentication
INFLUX_DB_HOST = "your-influxdb-host"
INFLUX_DB_USERNAME = "your-username"
INFLUX_DB_PASSWORD = "your-password"

# Execute multiprocessed data fetching
# (See notebook for complete implementation)
```

### Running Project 02 Performance Comparison

#### Pandas Version:
```python
import pandas as pd
import time

start = time.time()
logs_balancing_cell_voltages = pd.read_csv('log_balancing_cell_voltages copy.csv')
logs_balancing_cell_voltages = parse_logline(logs_balancing_cell_voltages)
print(f'Pandas processing time: {time.time() - start}s')
```

#### Polars Version:
```python
import polars as pl
import time

start = time.time()
logs_balancing_cell_voltages = pl.read_csv('log_balancing_cell_voltages copy.csv')
logs_balancing_cell_voltages = parse_logline_polars(logs_balancing_cell_voltages)
print(f'Polars processing time: {time.time() - start}s')
```

#### Spark Version:
```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName('Log Balancing Cell Voltages').getOrCreate()
start = time.time()
logs_balancing_cell_voltages = spark.read.option('header', 'true').csv('log_balancing_cell_voltages copy.csv')
logs_balancing_cell_voltages = parse_logline(logs_balancing_cell_voltages)
print(f'Spark processing time: {time.time() - start}s')
```

## Performance Insights

### Project 01 Optimizations
- **Multiprocessing**: Significantly reduces data fetching time through concurrent operations
- **Delta Tables**: Provides efficient storage and querying capabilities
- **Logging**: Enables monitoring and debugging of long-running processes

### Project 02 Framework Comparison
- **Pandas**: Best for small datasets and exploratory analysis
- **Polars**: Excellent performance for medium-scale data with lower memory usage
- **Spark**: Optimal for large-scale distributed processing

## Data Pipeline Architecture

### Project 01 Architecture
```
Data Sources → InfluxDB → Multiprocessed Fetching → Spark Processing → Delta Tables
```

### Project 02 Architecture
```
CSV Logs → Framework-Specific Processing → Structured Data → Parquet Storage
```

## Contributing

When contributing to this repository:
1. Follow the existing code structure and naming conventions
2. Include comprehensive logging for new features
3. Add performance measurements for new implementations
4. Update documentation for any new functionality

## License

This project is intended for educational and demonstration purposes.
