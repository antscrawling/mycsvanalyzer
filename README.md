# DuckDB Retail Analytics System

A comprehensive retail sales data analytics system built with DuckDB, Python, and FastAPI for time-series analysis and business intelligence.

## Features

### 🏪 Core Functionality
- **Retail Sales Data Generation**: Generate realistic time-series sales data with customer demographics
- **DuckDB Integration**: High-performance analytics with columnar database
- **Memory Optimization**: Efficient processing of large datasets (see [MEMORY_OPTIMIZATION.md](MEMORY_OPTIMIZATION.md))
- **REST API**: FastAPI-based web service for data access
- **Interactive Analytics**: Menu-driven analysis tools

### 📊 Analytics Capabilities
- Customer demographics analysis
- Product performance tracking
- Geographic sales distribution
- Hourly sales patterns
- Discount effectiveness analysis
- Income level segmentation
- Time-series trend analysis

### 🔧 Data Processing
- CSV file splitting and merging
- Parquet file conversion
- Data compression (Zlib)
- Batch processing for large datasets

## Installation

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)

### Setup
```bash
# Clone the repository
git clone <your-repo-url>
cd myduckdbdata

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### 1. Generate Sample Data
```bash
python src/main.py
# Select option 1 to generate sample dataset
```

### 2. Start the Retail Menu System
```bash
python src/retail_menu.py
```

### 3. Launch REST API
```bash
python src/app.py
# API documentation available at http://localhost:8000/docs
```

## Project Structure

```
src/
├── main.py                     # Core data generation and database operations
├── app.py                      # FastAPI REST API server
├── retail_menu.py              # Interactive menu system
├── analyze_csv.py              # CSV analysis utilities
├── final_summary.py            # Comprehensive reporting
├── discount_report.py          # Discount effectiveness analysis
├── visualization_insights.py   # Business insights generator
├── csv_split_to_csv_files.py  # CSV processing utilities
├── csv_split_to_parquet_files.py # Parquet conversion
└── sales_timeseries.db         # Main DuckDB database

test/
├── test_system.py              # System integration tests
├── test_api.py                 # API endpoint tests
└── test_script.py              # Basic functionality tests
```

## Usage Examples

### Data Generation
```python
from src.main import generate_initial_data2

# Generate sales data for date range
result = generate_initial_data2(
    start_iteration="2024-01-01 00:00:00",
    end_iteration="2024-01-31 23:59:59"
)
```

### Database Queries
```python
import duckdb

with duckdb.connect('src/sales_timeseries.db') as con:
    # Get top products by revenue
    top_products = con.execute("""
        SELECT product_name, SUM(total_amount_per_product_sgd) as revenue
        FROM sales_data 
        GROUP BY product_name 
        ORDER BY revenue DESC 
        LIMIT 10
    """).fetchall()
```

### API Usage
```bash
# Get sales summary
curl http://localhost:8000/summary/

# Get sales for specific date
curl http://localhost:8000/sales/by-date/2024-01-01

# Get top products
curl http://localhost:8000/analytics/top-products/
```

## API Endpoints

The FastAPI server provides comprehensive REST endpoints:

- **GET** `/summary/` - Overall sales statistics
- **GET** `/sales/` - Paginated sales data with filters
- **GET** `/sales/by-date/{date}` - Sales for specific date
- **GET** `/customers/` - Customer information
- **GET** `/analytics/top-products/` - Product performance
- **GET** `/analytics/demographics/` - Customer demographics
- **GET** `/analytics/hourly-distribution/` - Hourly sales patterns

Full API documentation: `http://localhost:8000/docs`

## Menu System Options

The interactive retail menu ([`retail_menu.py`](src/retail_menu.py)) provides:

1. **Database Recreation** - Generate new sample data
2. **Hourly Analysis** - Sales patterns by hour
3. **Discount Reports** - Discount effectiveness analysis  
4. **Retail Dashboard** - Comprehensive analytics
5. **Final Summary** - Business insights
6. **Export Functions** - Data export capabilities

## Testing

Run the test suite:
```bash
# System tests
python test/test_system.py

# API tests (requires running server)
python test/test_api.py

# Basic functionality
python test/test_script.py
```

## Data Schema

The main `sales_data` table includes:
- **Transaction Data**: date, transaction_id, transaction_desc
- **Customer Info**: customer_id, age, gender, income
- **Product Details**: product_id, product_name, units_sold, unit_price_sgd
- **Geographic**: country, city, country_id
- **Financial**: total_amount_per_product_sgd, receipt_total_sgd

## Performance Optimization

See [MEMORY_OPTIMIZATION.md](MEMORY_OPTIMIZATION.md) for detailed information on:
- Memory-efficient data generation
- Batch processing strategies
- Database indexing for performance
- Large dataset handling

## Configuration

Key configuration files:
- [`pyproject.toml`](pyproject.toml) - Project metadata
- [`requirements.txt`](requirements.txt) - Python dependencies
- [`src/cities.json`](src/cities.json) - Geographic data
- Parameter databases for analysis configuration

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests to ensure functionality
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the test files for usage examples
- Review the memory optimization guide
- Examine the API documentation at `/docs`