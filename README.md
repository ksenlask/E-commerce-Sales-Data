# E-commerce Sales Data Analytics Pipeline

A production-ready PySpark-based data processing pipeline that transforms raw e-commerce data into analytics-ready insights. Developed using Test-Driven Development (TDD) principles for reliability, scalability, and maintainability.

---

## Project Overview

This project demonstrates a complete data pipeline for e-commerce analytics, processing customer, product, and order data to generate actionable insights for marketing, finance, and executive reporting.

### Key Deliverables

- **Multi-format Data Ingestion**: Seamlessly processes CSV, JSON, and Excel files
- **Customer Analytics**: Advanced segmentation and behavioral analysis
- **Product Performance**: Comprehensive product and category analytics
- **Financial Intelligence**: Profit analysis and multi-dimensional aggregations
- **SQL-Ready Analytics**: Production-ready queries for business intelligence
- **Quality Assurance**: 122 automated tests ensuring data accuracy and reliability

### Quick Start

```bash
# 1. Clone and navigate to project
cd "E-commerce Sales Data"

# 2. Activate virtual environment
source pyspark-env/bin/activate

# 3. Run all tests (122 tests)
pytest tests/ -v

# 4. Run main pipeline
cd src && python main.py

# 5. Or explore notebooks
jupyter notebook src/notebooks/
```

---

## Assignment Approach & Implementation

### Development Strategy

This project was developed with a focus on **production-ready code quality** and **comprehensive testing**:

1. **Test-Driven Development (TDD)**: Tests written alongside implementation to ensure correctness
2. **Modular Design**: Separated concerns between data loading, cleaning, enrichment, and aggregation
3. **Error Handling**: Implemented graceful error handling with informative messages
4. **Documentation**: Comprehensive docstrings, inline comments, and this README

### Task Completion Status

#### Task 1: Raw Data Ingestion (100% Complete)

**Status**: Fully implemented and tested

**Implementation Highlights**:
- Multi-format data loading (CSV, JSON, Excel)
- Schema validation and data quality checks
- Explicit schema definitions for type safety
- SQL view creation for downstream analytics

**Files**:
- `src/load_source_data.py` - Core loading functions
- `notebooks/create_raw_tables.ipynb` - Interactive demonstration

**Test Results**:
- 45 tests for schema validation and data loading
- All tests passing

#### Task 2: Enriched Customer & Product Tables (100% Complete)

**Status**: Fully implemented and tested

**Customer Enrichments**:
- `total_sales`, `total_profit`, `total_orders` - Lifetime value metrics
- `average_order_value` - Basket size analysis
- `unique_products_purchased` - Product diversity
- `churn_risk_score` - Customer retention analysis (Low/Medium/High Risk)

**Product Enrichments**:
- `price_position` - Price tier classification (Premium/Mid-Range/Budget)
- `inventory_velocity` - Sales velocity (Fast/Medium/Slow Moving)

**Files**:
- `src/load_enriched_table.py` - Enrichment functions
- `src/data_cleaning_utils.py` - Data quality utilities
- `notebooks/create_enriched_table.ipynb` - Interactive demonstration

#### Task 3: Enriched Orders Table (100% Complete)

**Status**: Fully implemented and tested

**Enriched Columns**:
- Order information with **Profit rounded to 2 decimal places**
- **Customer Name** and **Country**
- **Product Category** and **Sub-Category**

**Files**:
- `src/load_enriched_orders.py` - Order enrichment logic
- `notebooks/create_enriched_orders.ipynb` - Interactive demonstration

#### Task 4: Profit Summary Aggregation (100% Complete)

**Status**: Fully implemented and tested

**Aggregation Dimensions**:
- Year (extracted from Order Date)
- Product Category
- Product Sub-Category
- Customer Name

**Output Columns**: `Year`, `Category`, `Sub-Category`, `Customer Name`, `Total_Profit`, `Order_Count`

**Files**:
- `src/load_profit_summary.py` - Aggregation logic
- `notebooks/create_profit_summary.ipynb` - Interactive demonstration

#### Task 5: SQL Profit Aggregates (100% Complete)

**Status**: Fully implemented and tested

**SQL Queries Implemented**:
1. **Profit by Year** - Annual profit trends
2. **Profit by Year + Product Category** - Category performance over time
3. **Profit by Customer** - Customer profitability ranking
4. **Profit by Customer + Year** - Customer trends over time

**Files**:
- `src/sql_profit_aggregates.py` - SQL query functions
- `notebooks/sql_profit_aggregates.ipynb` - Interactive demonstration

---

## Project Structure

```
E-commerce Sales Data/
├── data/                              # Source data files
│   ├── Customer.xlsx                  # Customer demographics (793 records)
│   ├── Orders.json                    # Transaction records (9,994 orders)
│   └── Products.csv                   # Product catalog (1,851 products)
├── src/                               # Core business logic
│   ├── __init__.py                    # Package initialization
│   ├── spark_session.py               # Spark session configuration
│   ├── load_source_data.py            # Raw data loading with schema validation
│   ├── data_cleaning_utils.py         # Reusable data cleaning functions
│   ├── load_enriched_table.py         # Customer & product enrichment
│   ├── load_enriched_orders.py        # Order enrichment with joins
│   ├── load_profit_summary.py         # Multi-dimensional profit aggregation
│   ├── sql_profit_aggregates.py       # SQL-based profit queries
│   ├── main.py                        # Main pipeline orchestration
│   ├── utils.py                       # Utility functions
│   └── notebooks/                     # Interactive Jupyter notebooks
│       ├── create_raw_tables.ipynb        # Task 1: Raw data loading
│       ├── create_enriched_table.ipynb    # Task 2: Customer/Product enrichment
│       ├── create_enriched_orders.ipynb   # Task 3: Order enrichment
│       ├── create_profit_summary.ipynb    # Task 4: Profit aggregation
│       └── sql_profit_aggregates.ipynb    # Task 5: SQL queries
├── tests/                             # Comprehensive test suite
│   ├── conftest.py                    # Shared pytest fixtures
│   ├── test_load_source_data.py       # 45 tests for data loading
│   ├── test_load_enriched_table.py    # 17 tests for enrichment
│   ├── test_load_enriched_orders.py   # 5 tests for order enrichment
│   ├── test_load_profit_summary.py    # 16 tests for profit summary
│   └── test_sql_profit_aggregates.py  # 39 tests for SQL aggregates
├── pyspark-env/                       # Python virtual environment
├── requirements.txt                   # Python dependencies
├── pytest.ini                         # Pytest configuration
└── README.md                          # This comprehensive guide
```

---

## Setup and Installation

### Prerequisites

- **Python 3.10+** (Tested with Python 3.10, 3.12)
- **Java 8 or 11** (Required for PySpark)
- **Minimum 4GB RAM** (8GB recommended)

### Installation

```bash
# 1. Clone repository and navigate to project
cd "E-commerce Sales Data"

# 2. Create virtual environment (if not exists)
python -m venv pyspark-env

# 3. Activate virtual environment
source pyspark-env/bin/activate  # macOS/Linux
# or
.\pyspark-env\Scripts\activate   # Windows

# 4. Install dependencies
pip install -r requirements.txt

# 5. Verify installation
python -c "import pyspark, pandas; print('Dependencies installed successfully')"
```

### Running the Pipeline

#### Option 1: Main Pipeline Script
```bash
cd src
python main.py
```

#### Option 2: Interactive Notebooks
```bash
jupyter notebook src/notebooks/
# Open notebooks in order: Task 1 → Task 2 → Task 3 → Task 4 → Task 5
```

#### Option 3: Run Tests
```bash
# Run all 122 tests
pytest tests/ -v

# Run specific test file
pytest tests/test_load_source_data.py -v

# Run with coverage
pytest tests/ -v --tb=short
```

---

## Business Use Cases

### Task 1: Multi-Format Data Ingestion
**Purpose**: Consolidate data from different business systems
- **CSV Processing**: Product catalogs and inventory data
- **JSON Processing**: Real-time order and transaction data
- **Excel Processing**: Customer demographics and contact information
- **Quality Assurance**: Automated schema validation and data quality checks

### Task 2: Customer & Product Intelligence
**Purpose**: Advanced analytics for marketing and merchandising teams
- **Customer Segmentation**: High-value customer identification and behavioral analysis
- **Product Performance**: Price tier classification and sales velocity analysis
- **Churn Prediction**: Risk scoring based on purchase recency

### Task 3: Order Enrichment & Processing
**Purpose**: Complete 360-degree view of business transactions
- **Data Integration**: Seamless joining of customer, product, and order data
- **Financial Accuracy**: Precise profit calculations with 2-decimal precision
- **Temporal Analysis**: Order date processing for trend analysis

### Task 4: Executive Reporting & Aggregations
**Purpose**: Strategic insights for executive decision-making
- **Multi-Dimensional Analysis**: Profit by Year, Category, Sub-Category, and Customer
- **Performance Trends**: Year-over-year growth analysis
- **Market Intelligence**: Category and customer profitability insights

### Task 5: Advanced SQL Analytics
**Purpose**: Self-service analytics for business users
- **Profit by Year**: Annual performance tracking
- **Profit by Year + Category**: Category trends over time
- **Profit by Customer**: Customer value ranking
- **Profit by Customer + Year**: Customer lifecycle analysis

---

## Key Assumptions and Design Decisions

### Data Processing Assumptions

#### Financial Calculations
- **Assumption**: All profit values rounded to **2 decimal places**
- **Reasoning**: Standard accounting practice
- **Impact**: Ensures consistent financial reporting

#### Data Quality Standards
- **Assumption**: Negative profits indicate returns/losses and are filtered for enrichment
- **Reasoning**: Focus on positive revenue transactions for customer/product metrics
- **Impact**: Cleaner aggregations for business insights

#### Date Formats
- **Assumption**: Order dates follow **d/M/yyyy** format
- **Reasoning**: Consistent with source data format
- **Impact**: Enables proper date parsing and year extraction

### Technical Implementation Assumptions

#### Schema Enforcement
- **Assumption**: Explicit schemas defined for all data sources
- **Reasoning**: Prevents schema inference overhead and ensures type safety
- **Impact**: Faster loading and predictable data types

#### Join Strategy
- **Assumption**: Inner joins used for enrichment to exclude orphaned records
- **Reasoning**: Ensures referential integrity in enriched tables
- **Impact**: Clean, complete records in output

### Business Logic Assumptions

#### Customer Churn Risk
- **Assumption**: Risk based on days since last order
  - Low Risk: < 90 days
  - Medium Risk: 90-180 days
  - High Risk: > 180 days
- **Reasoning**: Industry-standard retention thresholds

#### Product Price Tiers
- **Assumption**: Quartile-based classification within each category
  - Premium: Top 25%
  - Mid-Range: Middle 50%
  - Budget: Bottom 25%
- **Reasoning**: Relative pricing within product categories

---

## Test-Driven Development Approach

### Testing Philosophy

This project implements Test-Driven Development (TDD) principles:

**Why TDD for Data Pipelines?**
- **Early Bug Detection**: Issues caught before reaching production
- **Refactoring Confidence**: Safe code improvements with test validation
- **Living Documentation**: Tests document expected behavior
- **Regression Prevention**: Automated validation prevents breaking changes

### Test Coverage

| Test Suite | Focus Area | Test Count | Status |
|------------|------------|------------|--------|
| `test_load_source_data.py` | Schema validation, data loading | 45 | Passing |
| `test_load_enriched_table.py` | Customer & product enrichment | 17 | Passing |
| `test_load_enriched_orders.py` | Order enrichment, joins | 5 | Passing |
| `test_load_profit_summary.py` | Profit aggregation | 16 | Passing |
| `test_sql_profit_aggregates.py` | SQL queries, validation | 39 | Passing |

**Total: 122 tests across 5 test files - All Passing**

### What the Tests Validate

- Schema correctness for all data sources
- Data type validation (StringType, DoubleType, IntegerType, DateType)
- Business rule enforcement (positive quantities, valid dates)
- Join accuracy and referential integrity
- Profit calculation accuracy (2 decimal places)
- Aggregation correctness across dimensions
- Edge cases (empty DataFrames, null values, duplicates)

### Running Tests

```bash
# Run all tests with verbose output
pytest tests/ -v

# Run with short traceback
pytest tests/ -v --tb=short

# Run specific test class
pytest tests/test_load_source_data.py::TestCustomerSchemaValidation -v

```

---

## Data Quality Framework

### Data Cleaning Rules

| Rule | Table | Description |
|------|-------|-------------|
| Remove negative profits | Orders | Filter orders with Profit < 0 |
| Remove NULL Order IDs | Orders | Ensure every order has identifier |
| Remove duplicate Order IDs | Orders | Keep first occurrence |
| Validate date formats | Orders | Ensure d/M/yyyy format |
| Remove NULL Customer IDs | Customers, Orders | Ensure referential integrity |
| Remove duplicate Customer IDs | Customers | Keep first occurrence |
| Remove NULL Product IDs | Products, Orders | Ensure referential integrity |
| Remove duplicate Product IDs | Products | Keep first occurrence |

### Validation Functions

Each module includes validation functions that check:

```python
# Example validation from load_enriched_orders.py
def validate_enriched_orders_table(enriched_orders_df, original_orders_df):
    """
    Checks for:
    - Row count consistency
    - No nulls in key columns (Order ID, Customer Name, Product Name)
    - No invalid quantities (must be > 0)
    - No negative prices
    """
```

---

## Technical Specifications

### PySpark Configuration

```python
# Optimized Spark configuration
def get_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
```

### Performance Considerations

- **Schema Enforcement**: Explicit schemas prevent costly schema inference
- **Column Pruning**: `.select()` used to select only needed columns
- **Efficient Joins**: Inner joins with proper key columns
- **Partitioning**: Appropriate shuffle partitions for local execution

### Data Quality Framework

- **Schema Validation**: Automated checking for all data sources
- **Business Rule Enforcement**: Validation of constraints and logic
- **Error Handling**: Graceful error handling with informative messages

---

## Module Documentation

### load_source_data.py
Functions for loading raw data from CSV, JSON, and Excel with explicit schema validation.
- `load_customer_data()` - Load customer Excel file
- `load_orders_data()` - Load orders JSON file
- `load_products_data()` - Load products CSV file
- `perform_data_quality_checks()` - Comprehensive quality reporting

### data_cleaning_utils.py
Reusable data cleaning functions for consistent quality across modules.
- `clean_orders_for_enrichment()` - Apply all order cleaning rules
- `clean_customers_for_enrichment()` - Apply customer cleaning rules
- `clean_products_for_enrichment()` - Apply product cleaning rules

### load_enriched_table.py
Customer and product enrichment with business metrics.
- `summarize_customer_spending()` - Lifetime value metrics
- `calculate_average_basket_size()` - AOV calculation
- `identify_at_risk_customers()` - Churn risk scoring
- `classify_product_price_level()` - Price tier classification
- `identify_fast_and_slow_sellers()` - Inventory velocity

### load_enriched_orders.py
Creates denormalized sales overview by joining orders, customers, products.
- `create_enriched_orders_table()` - Main enrichment function
- `validate_enriched_orders_table()` - Quality validation

### load_profit_summary.py
Multi-dimensional profit aggregation.
- `create_profit_summary_table()` - Aggregate by Year, Category, Sub-Category, Customer
- `validate_profit_summary_table()` - Validation function

### sql_profit_aggregates.py
SQL-based profit analysis queries.
- `get_profit_by_year()` - Annual profit
- `get_profit_by_year_category()` - Year + Category profit
- `get_profit_by_customer()` - Customer profit ranking
- `get_profit_by_customer_year()` - Customer + Year profit
- `validate_aggregate_output()` - Query result validation

---

## Key Takeaways

**What This Project Demonstrates**:

1. **Production-Ready Code**: Modular, well-documented, error-handled
2. **Test-Driven Development**: 122 comprehensive tests with full coverage
3. **Data Quality Focus**: Multiple validation layers and cleaning rules
4. **Clear Documentation**: Comprehensive README, docstrings, and inline comments
5. **Scalable Architecture**: PySpark-based for distributed processing
6. **Business Value**: Actionable insights for marketing, finance, and operations

**Technical Skills Showcased**:

- PySpark DataFrame operations and transformations
- Multi-format data ingestion (CSV, JSON, Excel)
- SQL query development and optimization
- Test-driven development with pytest
- Data quality and validation frameworks
- Documentation and technical writing

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| pyspark | 3.5.1 | Distributed data processing |
| pandas | 2.3+ | Excel file reading, data manipulation |
| openpyxl | 3.1+ | Excel file support |
| pytest | 9.0+ | Testing framework |
| numpy | 2.2+ | Numerical operations |

---

**Version**: 1.0.0  
**Last Updated**: November 2025  
**Environment**: macOS/Linux, Python 3.10+, PySpark 3.5.1  
**Status**: All 5 Tasks Complete, 122 Tests Passing  
**Author**: E-commerce Analytics Team
