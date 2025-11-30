import sys
import os
from datetime import date

# =====================================================
# Add project root to Python path BEFORE any imports
# =====================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest
from src.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import to_date, col

@pytest.fixture(scope="session")
def spark():
    """Shared Spark session for all tests."""
    spark = get_spark_session("PyTestSession")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def sample_data(spark):
    """Provides sample dataframes for testing enrichment and aggregation."""
    customers_data = [("C1", "Kushal Sen Laskar", "India"), ("C2", "John Doe", "USA")]
    customers_schema = StructType([
        StructField("Customer ID", StringType()),
        StructField("Customer Name", StringType()),
        StructField("Country", StringType())
    ])
    customers_df = spark.createDataFrame(customers_data, customers_schema)

    products_data = [("P1", "Laptop", "Technology", "Computers"), ("P2", "Mouse", "Technology", "Accessories")]
    products_schema = StructType([
        StructField("Product ID", StringType()),
        StructField("Product Name", StringType()),
        StructField("Category", StringType()),
        StructField("Sub-Category", StringType())
    ])
    products_df = spark.createDataFrame(products_data, products_schema)

    orders_data = [
        ("O1", "21/8/2016", "25/8/2016", "Standard Class", "C1", "P1", 1, 1200.555, 0.0, 120.123),
        ("O2", "23/9/2017", "29/9/2017", "Second Class", "C2", "P2", 2, 50.0, 0.0, 5.456),
        ("O3", "1/1/2017", "5/1/2017", "First Class", "C1", "P2", 1, 25.0, 0.0, 2.5),
        ("O4", "10/2/2018", "15/2/2018", "Standard Class", "C99", "P1", 1, 1200.0, 0.0, 120.0) # Orphaned order
    ]
    orders_schema = StructType([
        StructField("Order ID", StringType()),
        StructField("Order Date", StringType()),
        StructField("Ship Date", StringType()),
        StructField("Ship Mode", StringType()),
        StructField("Customer ID", StringType()),
        StructField("Product ID", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("Price", DoubleType()),
        StructField("Discount", DoubleType()),
        StructField("Profit", DoubleType())
    ])
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    
    return {"customers_df": customers_df, "products_df": products_df, "orders_df": orders_df}


@pytest.fixture(scope="session")
def sample_data_with_dates(spark):
    """Provides enriched orders DataFrame with proper date types for profit summary testing."""
    enriched_orders_data = [
        ("O1", date(2016, 8, 21), "25/8/2016", "Standard Class", "Kushal Sen Laskar", "India", "Technology", "Computers", "Laptop", 1, 1200.555, 0.0, 120.123),
        ("O2", date(2017, 9, 23), "29/9/2017", "Second Class", "John Doe", "USA", "Technology", "Accessories", "Mouse", 2, 50.0, 0.0, 5.456),
        ("O3", date(2017, 1, 1), "5/1/2017", "First Class", "Kushal Sen Laskar", "India", "Technology", "Accessories", "Keyboard", 1, 25.0, 0.0, 2.5),
        ("O4", date(2018, 2, 10), "15/2/2018", "Standard Class", "John Doe", "USA", "Office Supplies", "Binders", "Binder", 1, 1200.0, 0.0, 120.0)
    ]
    enriched_orders_schema = StructType([
        StructField("Order ID", StringType()),
        StructField("Order Date", DateType()),
        StructField("Ship Date", StringType()),
        StructField("Ship Mode", StringType()),
        StructField("Customer Name", StringType()),
        StructField("Country", StringType()),
        StructField("Category", StringType()),
        StructField("Sub-Category", StringType()),
        StructField("Product Name", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("Price", DoubleType()),
        StructField("Discount", DoubleType()),
        StructField("Profit", DoubleType())
    ])
    
    # Create the dataframe with DateType columns
    enriched_orders_df = spark.createDataFrame(enriched_orders_data, enriched_orders_schema)
    
    return {"enriched_orders_df": enriched_orders_df}

