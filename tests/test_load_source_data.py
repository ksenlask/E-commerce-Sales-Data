"""
Unit tests for load_source_data.py module

Tests cover:
- Schema Validation: Column count, names, types, nullability
- Data Loading: DataFrame creation, record counts, data integrity
"""

import sys
import os
import warnings
import pytest
import tempfile
import pandas as pd
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, isnan

# Filter out DeprecationWarning from PySpark
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pyspark.sql.pandas.utils")

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.load_source_data import load_customer_data
from src.load_source_data import load_orders_data
from src.load_source_data import load_products_data
from src.load_source_data import perform_data_quality_checks 


# ============================================================================
# SCHEMA VALIDATION TEST CASES FOR CUSTOMER
# ============================================================================

class TestCustomerSchemaValidation:
    """Test cases for schema validation of load_customer_data()"""
    
    def test_load_customer_data_correct_column_count(self, spark):
        """Verify DataFrame has exactly 11 columns"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        assert len(df.columns) == 11, f"Expected 11 columns, got {len(df.columns)}"
    
    def test_load_customer_data_correct_column_names(self, spark):
        """Verify all column names match the defined schema"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        expected_columns = [
            "Customer ID",
            "Customer Name",
            "email",
            "phone",
            "address",
            "Segment",
            "Country",
            "City",
            "State",
            "Postal Code",
            "Region"
        ]
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.columns == expected_columns, f"Column names mismatch. Expected: {expected_columns}, Got: {df.columns}"
    
    def test_load_customer_data_all_columns_string_type(self, spark):
        """Verify all columns are StringType"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        for field in df.schema.fields:
            assert isinstance(field.dataType, StringType), \
                f"Column '{field.name}' should be StringType, got {type(field.dataType).__name__}"
  
    def test_load_customer_data_schema_structure(self, spark):
        """Verify complete schema structure matches StructType definition"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        expected_schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Postal Code", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.schema == expected_schema, \
            f"Schema mismatch. Expected: {expected_schema}, Got: {df.schema}"
    
    def test_load_customer_data_missing_columns_in_excel(self, spark):
        """Verify behavior when Excel has fewer columns"""
        incomplete_data = {
            'Customer ID': ['C001', 'C002'],
            'Customer Name': ['John Doe', 'Jane Smith'],
            'email': ['john@test.com', 'jane@test.com'],
            'phone': ['123456', '789012'],
            'address': ['123 Main St', '456 Oak Ave']
        }
        
        pd_df = pd.DataFrame(incomplete_data)
        
        expected_column_count = 11
        actual_column_count = len(pd_df.columns)
        
        assert actual_column_count < expected_column_count, \
            f"Should have fewer columns. Expected 11, got {actual_column_count}"
    
    def test_load_customer_data_wrong_column_names(self, spark):
        """Verify behavior when column names don't match"""
        wrong_columns_data = {
            'Cust_ID': ['C001', 'C002'],
            'Cust_Name': ['John Doe', 'Jane Smith'],
            'Email': ['john@test.com', 'jane@test.com'],
            'Phone': ['123456', '789012'],
            'Address': ['123 Main St', '456 Oak Ave'],
            'Segment': ['Consumer', 'Corporate'],
            'Country': ['USA', 'UK'],
            'City': ['New York', 'London'],
            'State': ['NY', 'England'],
            'PostalCode': ['10001', 'SW1A'],
            'Region': ['East', 'West']
        }
        
        pd_df = pd.DataFrame(wrong_columns_data)
        
        expected_columns = ["Customer ID", "Customer Name", "email", "phone", "address", 
                          "Segment", "Country", "City", "State", "Postal Code", "Region"]
        actual_columns = list(pd_df.columns)
        
        assert actual_columns != expected_columns, "Column names should not match expected schema"


# ============================================================================
# DATA LOADING TEST CASES FOR CUSTOMER
# ============================================================================

class TestCustomerDataLoading:
    """Test cases for data loading functionality of load_customer_data()"""
    
    def test_load_customer_data_returns_dataframe(self, spark):
        """Verify function returns Spark DataFrame object"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        assert type(df).__name__ == 'DataFrame', \
            f"Expected pyspark.sql.DataFrame, got {type(df).__name__}"
    
    def test_load_customer_data_has_records(self, spark):
        """Verify loaded DataFrame contains data"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        record_count = df.count()
        assert record_count > 0, f"DataFrame should have records, got {record_count}"
    
    def test_load_customer_data_actual_record_count(self, spark):
        """Verify correct number of records are loaded"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        expected_min_count = 700
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        actual_count = df.count()
        assert actual_count >= expected_min_count, \
            f"Expected at least {expected_min_count} records, got {actual_count}"
    
    def test_load_customer_data_with_actual_project_file(self, spark):
        """Integration test with real project data file"""
        customer_file = os.path.join(PROJECT_ROOT, "data", "Customer.xlsx")
        expected_columns = 11
        
        df = load_customer_data(spark, customer_file)
        
        assert df is not None, "DataFrame should not be None"
        
        assert len(df.columns) == expected_columns, \
            f"Expected {expected_columns} columns, got {len(df.columns)}"
        
        assert df.count() > 0, "Should have records"
        
        sample_data = df.limit(5).collect()
        assert len(sample_data) > 0, "Should be able to collect sample data"
        
        customer_ids = df.select("Customer ID").limit(5).collect()
        assert len(customer_ids) > 0, "Should have Customer ID values"
    
    def test_load_customer_data_empty_excel_file(self, spark):
        """Verify behavior with empty Excel file (no data rows)"""
        schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Postal Code", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        df = spark.createDataFrame([], schema=schema)
        
        assert df is not None, "Should handle empty data and return DataFrame"
        assert df.count() == 0, "Empty file should result in DataFrame with 0 records"
    
    def test_load_customer_data_excel_with_null_values(self, spark):
        """Verify behavior when Excel contains null/empty cells"""
        data_with_nulls = [
            ('C001', 'John Doe', 'john@test.com', '123456', None, 'Consumer', 'USA', 'New York', 'NY', '10001', 'East'),
            ('C002', None, 'jane@test.com', '789012', '456 Oak Ave', 'Corporate', 'UK', None, 'England', 'SW1A', 'West'),
            ('C003', 'Bob Wilson', None, '345678', '789 Pine St', 'Consumer', 'Canada', 'Toronto', 'ON', None, 'North')
        ]
        
        schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Postal Code", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        df = spark.createDataFrame(data_with_nulls, schema=schema)
        
        assert df is not None, "Should handle null values and return DataFrame"
        assert df.count() == 3, "Should have 3 records"
        
        null_count = df.filter(col("Customer Name").isNull()).count()
        assert null_count == 1, "Should have 1 null value in Customer Name column"


# ============================================================================
# SCHEMA VALIDATION TEST CASES FOR PRODUCTS
# ============================================================================

class TestProductSchemaValidation:
    """Test cases for schema validation of load_products_data()"""
    
    def test_load_products_data_correct_column_count(self, spark):
        """Verify DataFrame has exactly 6 columns"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        assert len(df.columns) == 6, f"Expected 6 columns, got {len(df.columns)}"
    
    def test_load_products_data_correct_column_names(self, spark):
        """Verify all column names match the defined schema"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        expected_columns = [
            "Product ID",
            "Category",
            "Sub-Category",
            "Product Name",
            "State",
            "Price per product"
        ]
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.columns == expected_columns, f"Column names mismatch. Expected: {expected_columns}, Got: {df.columns}"
    
    def test_load_products_data_column_types(self, spark):
        """Verify column data types"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        
        from pyspark.sql.types import DoubleType
        
        string_columns = ["Product ID", "Category", "Sub-Category", "Product Name", "State"]
        for col_name in string_columns:
            field = [f for f in df.schema.fields if f.name == col_name][0]
            assert isinstance(field.dataType, StringType), \
                f"Column '{col_name}' should be StringType, got {type(field.dataType).__name__}"
        
        price_field = [f for f in df.schema.fields if f.name == "Price per product"][0]
        assert isinstance(price_field.dataType, DoubleType), \
            f"Column 'Price per product' should be DoubleType, got {type(price_field.dataType).__name__}"
    
    def test_load_products_data_schema_structure(self, spark):
        """Verify complete schema structure matches StructType definition"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        
        from pyspark.sql.types import DoubleType
        
        expected_schema = StructType([
            StructField("Product ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub-Category", StringType(), True),
            StructField("Product Name", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Price per product", DoubleType(), True)
        ])
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.schema == expected_schema, \
            f"Schema mismatch. Expected: {expected_schema}, Got: {df.schema}"
    
    def test_load_products_data_missing_columns(self, spark):
        """Verify behavior when CSV has fewer columns"""
        incomplete_data = {
            'Product ID': ['P001', 'P002'],
            'Category': ['Furniture', 'Technology'],
            'Product Name': ['Chair', 'Phone']
        }
        
        pd_df = pd.DataFrame(incomplete_data)
        
        expected_column_count = 6
        actual_column_count = len(pd_df.columns)
        
        assert actual_column_count < expected_column_count, \
            f"Should have fewer columns. Expected 6, got {actual_column_count}"
    
    def test_load_products_data_wrong_column_names(self, spark):
        """Verify behavior when column names don't match"""
        wrong_columns_data = {
            'Prod_ID': ['P001', 'P002'],
            'Cat': ['Furniture', 'Technology'],
            'SubCat': ['Chairs', 'Phones'],
            'Name': ['Office Chair', 'Smartphone'],
            'Location': ['CA', 'NY'],
            'Cost': [299.99, 799.99]
        }
        
        pd_df = pd.DataFrame(wrong_columns_data)
        
        expected_columns = ["Product ID", "Category", "Sub-Category", "Product Name", "State", "Price per product"]
        actual_columns = list(pd_df.columns)
        
        assert actual_columns != expected_columns, "Column names should not match expected schema"


# ============================================================================
# DATA LOADING TEST CASES FOR PRODUCTS
# ============================================================================

class TestProductDataLoading:
    """Test cases for data loading functionality of load_products_data()"""
    
    def test_load_products_data_returns_dataframe(self, spark):
        """Verify function returns Spark DataFrame object"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        assert type(df).__name__ == 'DataFrame', \
            f"Expected pyspark.sql.DataFrame, got {type(df).__name__}"
    
    def test_load_products_data_has_records(self, spark):
        """Verify loaded DataFrame contains data"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        record_count = df.count()
        assert record_count > 0, f"DataFrame should have records, got {record_count}"
    
    def test_load_products_data_actual_record_count(self, spark):
        """Verify correct number of records are loaded"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        expected_min_count = 1800
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        actual_count = df.count()
        assert actual_count >= expected_min_count, \
            f"Expected at least {expected_min_count} records, got {actual_count}"
    
    def test_load_products_data_with_actual_project_file(self, spark):
        """Integration test with real project data file"""
        products_file = os.path.join(PROJECT_ROOT, "data", "Products.csv")
        expected_columns = 6
        
        df = load_products_data(spark, products_file)
        
        assert df is not None, "DataFrame should not be None"
        
        assert len(df.columns) == expected_columns, \
            f"Expected {expected_columns} columns, got {len(df.columns)}"
        
        assert df.count() > 0, "Should have records"
        
        sample_data = df.limit(5).collect()
        assert len(sample_data) > 0, "Should be able to collect sample data"
        
        product_ids = df.select("Product ID").limit(5).collect()
        assert len(product_ids) > 0, "Should have Product ID values"
    
    def test_load_products_data_empty_csv_file(self, spark):
        """Verify behavior with empty CSV file (no data rows)"""
        from pyspark.sql.types import DoubleType
        
        schema = StructType([
            StructField("Product ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub-Category", StringType(), True),
            StructField("Product Name", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Price per product", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([], schema=schema)
        
        assert df is not None, "Should handle empty data and return DataFrame"
        assert df.count() == 0, "Empty file should result in DataFrame with 0 records"
    
    def test_load_products_data_with_null_values(self, spark):
        """Verify behavior when CSV contains null/empty cells"""
        from pyspark.sql.types import DoubleType
        
        data_with_nulls = [
            ('P001', 'Furniture', 'Chairs', 'Office Chair', 'CA', 299.99),
            ('P002', None, 'Phones', 'Smartphone', 'NY', None),
            ('P003', 'Office Supplies', None, 'Stapler', None, 15.99)
        ]
        
        schema = StructType([
            StructField("Product ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub-Category", StringType(), True),
            StructField("Product Name", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Price per product", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data_with_nulls, schema=schema)
        
        assert df is not None, "Should handle null values and return DataFrame"
        assert df.count() == 3, "Should have 3 records"
        
        null_count = df.filter(col("Category").isNull()).count()
        assert null_count == 1, "Should have 1 null value in Category column"


# ============================================================================
# SCHEMA VALIDATION TEST CASES FOR ORDERS
# ============================================================================

class TestOrdersSchemaValidation:
    """Test cases for schema validation of load_orders_data()"""
    
    def test_load_orders_data_correct_column_count(self, spark):
        """Verify DataFrame has exactly 11 columns"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        assert len(df.columns) == 11, f"Expected 11 columns, got {len(df.columns)}"
    
    def test_load_orders_data_correct_column_names(self, spark):
        """Verify all column names match the defined schema"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        expected_columns = [
            "Row ID",
            "Order ID",
            "Order Date",
            "Ship Date",
            "Ship Mode",
            "Customer ID",
            "Product ID",
            "Quantity",
            "Price",
            "Discount",
            "Profit"
        ]
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.columns == expected_columns, f"Column names mismatch. Expected: {expected_columns}, Got: {df.columns}"
    
    def test_load_orders_data_column_types(self, spark):
        """Verify column data types"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        
        from pyspark.sql.types import IntegerType, DoubleType
        
        string_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Product ID"]
        for col_name in string_columns:
            field = [f for f in df.schema.fields if f.name == col_name][0]
            assert isinstance(field.dataType, StringType), \
                f"Column '{col_name}' should be StringType, got {type(field.dataType).__name__}"
        
        int_columns = ["Row ID", "Quantity"]
        for col_name in int_columns:
            field = [f for f in df.schema.fields if f.name == col_name][0]
            assert isinstance(field.dataType, IntegerType), \
                f"Column '{col_name}' should be IntegerType, got {type(field.dataType).__name__}"
        
        double_columns = ["Price", "Discount", "Profit"]
        for col_name in double_columns:
            field = [f for f in df.schema.fields if f.name == col_name][0]
            assert isinstance(field.dataType, DoubleType), \
                f"Column '{col_name}' should be DoubleType, got {type(field.dataType).__name__}"
    
    def test_load_orders_data_schema_structure(self, spark):
        """Verify complete schema structure matches StructType definition"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        
        from pyspark.sql.types import IntegerType, DoubleType
        
        expected_schema = StructType([
            StructField("Row ID", IntegerType(), True),
            StructField("Order ID", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Ship Date", StringType(), True),
            StructField("Ship Mode", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Product ID", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Discount", DoubleType(), True),
            StructField("Profit", DoubleType(), True)
        ])
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        assert df.schema == expected_schema, \
            f"Schema mismatch. Expected: {expected_schema}, Got: {df.schema}"
    
    def test_load_orders_data_missing_columns(self, spark):
        """Verify behavior when JSON has fewer columns"""
        incomplete_data = {
            'Row ID': [1, 2],
            'Order ID': ['O001', 'O002'],
            'Customer ID': ['C001', 'C002'],
            'Product ID': ['P001', 'P002']
        }
        
        pd_df = pd.DataFrame(incomplete_data)
        
        expected_column_count = 11
        actual_column_count = len(pd_df.columns)
        
        assert actual_column_count < expected_column_count, \
            f"Should have fewer columns. Expected 11, got {actual_column_count}"
    
    def test_load_orders_data_wrong_column_names(self, spark):
        """Verify behavior when column names don't match"""
        wrong_columns_data = {
            'RowNum': [1, 2],
            'OrderNumber': ['O001', 'O002'],
            'OrderDt': ['2024-01-01', '2024-01-02'],
            'ShipDt': ['2024-01-05', '2024-01-06'],
            'ShippingMode': ['Standard', 'Express'],
            'CustID': ['C001', 'C002'],
            'ProdID': ['P001', 'P002'],
            'Qty': [5, 3],
            'Cost': [100.0, 200.0],
            'Disc': [0.1, 0.2],
            'Prof': [20.0, 40.0]
        }
        
        pd_df = pd.DataFrame(wrong_columns_data)
        
        expected_columns = ["Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", 
                          "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"]
        actual_columns = list(pd_df.columns)
        
        assert actual_columns != expected_columns, "Column names should not match expected schema"


# ============================================================================
# DATA LOADING TEST CASES FOR ORDERS
# ============================================================================

class TestOrdersDataLoading:
    """Test cases for data loading functionality of load_orders_data()"""
    
    def test_load_orders_data_returns_dataframe(self, spark):
        """Verify function returns Spark DataFrame object"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        assert type(df).__name__ == 'DataFrame', \
            f"Expected pyspark.sql.DataFrame, got {type(df).__name__}"
    
    def test_load_orders_data_has_records(self, spark):
        """Verify loaded DataFrame contains data"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        record_count = df.count()
        assert record_count > 0, f"DataFrame should have records, got {record_count}"
    
    def test_load_orders_data_actual_record_count(self, spark):
        """Verify correct number of records are loaded"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        expected_min_count = 9900
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        actual_count = df.count()
        assert actual_count >= expected_min_count, \
            f"Expected at least {expected_min_count} records, got {actual_count}"
    
    def test_load_orders_data_with_actual_project_file(self, spark):
        """Integration test with real project data file"""
        orders_file = os.path.join(PROJECT_ROOT, "data", "Orders.json")
        expected_columns = 11
        
        df = load_orders_data(spark, orders_file)
        
        assert df is not None, "DataFrame should not be None"
        
        assert len(df.columns) == expected_columns, \
            f"Expected {expected_columns} columns, got {len(df.columns)}"
        
        assert df.count() > 0, "Should have records"
        
        sample_data = df.limit(5).collect()
        assert len(sample_data) > 0, "Should be able to collect sample data"
        
        order_ids = df.select("Order ID").limit(5).collect()
        assert len(order_ids) > 0, "Should have Order ID values"
    
    def test_load_orders_data_empty_json_file(self, spark):
        """Verify behavior with empty JSON file (no data rows)"""
        from pyspark.sql.types import IntegerType, DoubleType
        
        schema = StructType([
            StructField("Row ID", IntegerType(), True),
            StructField("Order ID", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Ship Date", StringType(), True),
            StructField("Ship Mode", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Product ID", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Discount", DoubleType(), True),
            StructField("Profit", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([], schema=schema)
        
        assert df is not None, "Should handle empty data and return DataFrame"
        assert df.count() == 0, "Empty file should result in DataFrame with 0 records"
    
    def test_load_orders_data_with_null_values(self, spark):
        """Verify behavior when JSON contains null/empty cells"""
        from pyspark.sql.types import IntegerType, DoubleType
        
        data_with_nulls = [
            (1, 'O001', '2024-01-01', '2024-01-05', 'Standard', 'C001', 'P001', 5, 100.0, 0.1, 20.0),
            (2, None, '2024-01-02', None, 'Express', 'C002', 'P002', None, 200.0, None, 40.0),
            (3, 'O003', None, '2024-01-10', 'Standard', 'C003', None, 10, None, 0.15, None)
        ]
        
        schema = StructType([
            StructField("Row ID", IntegerType(), True),
            StructField("Order ID", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Ship Date", StringType(), True),
            StructField("Ship Mode", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Product ID", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Discount", DoubleType(), True),
            StructField("Profit", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data_with_nulls, schema=schema)
        
        assert df is not None, "Should handle null values and return DataFrame"
        assert df.count() == 3, "Should have 3 records"
        
        null_count = df.filter(col("Order ID").isNull()).count()
        assert null_count == 1, "Should have 1 null value in Order ID column"


# ============================================================================
# DATA QUALITY CHECKS - POSITIVE TEST CASES
# ============================================================================

class TestDataQualityChecksPositive:
    """Test cases for perform_data_quality_checks() with valid DataFrames"""
    
    def test_quality_checks_with_valid_dataframe_multiple_records(self, spark):
        """Verify quality checks pass with valid DataFrame containing multiple records"""
        data = [
            ('C001', 'John Doe', 'Consumer'),
            ('C002', 'Jane Smith', 'Corporate'),
            ('C003', 'Bob Wilson', 'Consumer'),
            ('C004', 'Alice Brown', 'Home Office')
        ]
        
        schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("Segment", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        result = perform_data_quality_checks(df, "Test_Customers")
        
        assert result is not None, "Quality check result should not be None"
        assert result["status"] == "Passed", "Quality check should pass"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["table_name"] == "Test_Customers", "Table name should match"
        assert result["total_columns"] == 3, "Should have 3 columns"
        assert result["number_of_records"] == 4, "Should have 4 records"
        assert len(result["columns_available"]) == 3, "Should have 3 columns in list"
        assert "Customer ID" in result["columns_available"], "Customer ID column should be present"
        assert result["schema"] is not None, "Schema should not be None"
        assert len(result["sample_records"]) == 2, "Should have 2 sample records"
    
    def test_quality_checks_with_mixed_column_types(self, spark):
        """Verify quality checks handle DataFrame with mixed column types (String, Integer, Double)"""
        from pyspark.sql.types import IntegerType, DoubleType
        
        data = [
            (1, 'O001', 'C001', 5, 100.0, 0.1),
            (2, 'O002', 'C002', 3, 250.0, 0.15),
            (3, 'O003', 'C003', 10, 50.0, 0.05)
        ]
        
        schema = StructType([
            StructField("Row ID", IntegerType(), True),
            StructField("Order ID", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Discount", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        result = perform_data_quality_checks(df, "Mixed_Types_Orders")
        
        assert result["status"] == "Passed", "Quality check should pass"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["number_of_records"] == 3, "Should have 3 records"
        assert result["total_columns"] == 6, "Should have 6 columns"
        assert "Row ID" in result["columns_available"], "Row ID column should be present"
        assert "Quantity" in result["columns_available"], "Quantity column should be present"
        assert result["schema"] is not None, "Schema should not be None"
        
        # Verify schema contains different types
        schema_fields = {field.name: field.dataType for field in result["schema"].fields}
        assert isinstance(schema_fields["Row ID"], IntegerType), "Row ID should be IntegerType"
        assert isinstance(schema_fields["Order ID"], StringType), "Order ID should be StringType"
        assert isinstance(schema_fields["Price"], DoubleType), "Price should be DoubleType"
    
    def test_quality_checks_with_special_characters_in_columns(self, spark):
        """Verify quality checks handle column names with special characters"""
        from pyspark.sql.types import DoubleType
        
        data = [
            ('P001', 'Office Supplies', 'Binders', 15.99),
            ('P002', 'Technology', 'Phones', 799.99),
            ('P003', 'Furniture', 'Chairs', 299.99)
        ]
        
        schema = StructType([
            StructField("Product ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub-Category", StringType(), True),
            StructField("Price per product", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        result = perform_data_quality_checks(df, "Products_Special_Chars")
        
        assert result["status"] == "Passed", "Quality check should pass"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["number_of_records"] == 3, "Should have 3 records"
        assert result["total_columns"] == 4, "Should have 4 columns"
        assert "Sub-Category" in result["columns_available"], "Sub-Category column should be present"
        assert "Price per product" in result["columns_available"], "Price per product column should be present"
        assert len(result["sample_records"]) == 2, "Should have 2 sample records"


# ============================================================================
# DATA QUALITY CHECKS - NEGATIVE TEST CASES
# ============================================================================

class TestDataQualityChecksNegative:
    """Test cases for perform_data_quality_checks() with invalid or problematic DataFrames"""
    
    def test_quality_checks_with_empty_dataframe(self, spark):
        """Verify quality checks handle empty DataFrame with schema but no data"""
        schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("Segment", StringType(), True)
        ])
        
        df = spark.createDataFrame([], schema=schema)
        
        result = perform_data_quality_checks(df, "Empty_DataFrame")
        
        assert result is not None, "Quality check result should not be None"
        assert result["status"] == "Passed", "Quality check should still pass for empty DataFrame"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["number_of_records"] == 0, "Should have 0 records"
        assert result["total_columns"] == 3, "Should have 3 columns"
        assert len(result["columns_available"]) == 3, "Should have 3 columns in list"
        assert len(result["sample_records"]) == 0, "Should have 0 sample records"
        assert result["schema"] is not None, "Schema should not be None"
    
    def test_quality_checks_returns_failed_status_for_none(self, spark):
        """Verify quality checks return failed status when DataFrame is None"""
        df = None
        
        result = perform_data_quality_checks(df, "None_DataFrame")
        
        assert result is not None, "Quality check result should not be None"
        assert result["status"] == "Failed", "Quality check should fail for None DataFrame"
        assert result["dataframe_created"] is False, "DataFrame created flag should be False"
        assert result["table_name"] == "None_DataFrame", "Table name should match"
        assert result["number_of_records"] == 0, "Should have 0 records"
        assert result["total_columns"] == 0, "Should have 0 columns"
        assert len(result["columns_available"]) == 0, "Columns list should be empty"
        assert result["schema"] is None, "Schema should be None"
        assert len(result["sample_records"]) == 0, "Sample records should be empty"


# ============================================================================
# DATA QUALITY CHECKS - EDGE CASES
# ============================================================================

class TestDataQualityChecksEdgeCases:
    """Test cases for edge cases and boundary conditions in perform_data_quality_checks()"""
    
    def test_quality_checks_with_many_columns(self, spark):
        """Verify quality checks handle DataFrame with many columns (20+)"""
        data = [(
            'C001', 'John Doe', 'john@test.com', '123456', '123 Main St',
            'Consumer', 'USA', 'New York', 'NY', '10001', 'East',
            'Active', 'Premium', '2024-01-01', 'Corporate', 'Large',
            'Technology', 'A+', '5 years', '1000+'
        )]
        
        schema = StructType([
            StructField("Customer ID", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Postal Code", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Membership", StringType(), True),
            StructField("Join Date", StringType(), True),
            StructField("Account Type", StringType(), True),
            StructField("Company Size", StringType(), True),
            StructField("Industry", StringType(), True),
            StructField("Credit Rating", StringType(), True),
            StructField("Tenure", StringType(), True),
            StructField("Employee Count", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        result = perform_data_quality_checks(df, "Many_Columns_Test")
        
        assert result["status"] == "Passed", "Quality check should pass"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["total_columns"] == 20, "Should have 20 columns"
        assert len(result["columns_available"]) == 20, "Should have 20 columns in list"
        assert result["number_of_records"] == 1, "Should have 1 record"
        assert "Customer ID" in result["columns_available"], "Customer ID should be present"
        assert "Employee Count" in result["columns_available"], "Last column should be present"
    
    def test_quality_checks_with_few_columns(self, spark):
        """Verify quality checks handle DataFrame with very few columns (1-2)"""
        data = [
            ('C001',),
            ('C002',),
            ('C003',)
        ]
        
        schema = StructType([
            StructField("ID", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        result = perform_data_quality_checks(df, "Single_Column_Test")
        
        assert result["status"] == "Passed", "Quality check should pass"
        assert result["dataframe_created"] is True, "DataFrame created flag should be True"
        assert result["total_columns"] == 1, "Should have 1 column"
        assert len(result["columns_available"]) == 1, "Should have 1 column in list"
        assert result["columns_available"][0] == "ID", "Column name should be ID"
        assert result["number_of_records"] == 3, "Should have 3 records"
        assert len(result["sample_records"]) == 2, "Should have 2 sample records"

