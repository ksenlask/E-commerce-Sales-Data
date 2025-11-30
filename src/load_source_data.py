import os
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, isnan, when

def load_customer_data(spark, file_path):
    """Load customer Excel data using Spark."""
    
    # Define the customer schema
    customer_schema = StructType([
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

    print(f"\nChecking file at: {file_path}")

    if not os.path.exists(file_path):
        print("File not found!")
        return None

    print("File found. Loading Excel data using Spark...")

    try:
        # Read Excel file using pandas
        pandas_df = pd.read_excel(file_path)
        
        # Validate column names match expected schema
        expected_columns = [field.name for field in customer_schema.fields]
        actual_columns = list(pandas_df.columns)
        
        if actual_columns != expected_columns:
            print(f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}")
            return None

        # Convert pandas DataFrame to Spark DataFrame with defined schema
        df = spark.createDataFrame(pandas_df, schema=customer_schema)

        print("Customer data loaded successfully")

        # Print schema
        #print("\nSchema:")
        #df.printSchema()

        # Record count
        #count = df.count()
        #print(f"\nTotal Records: {count}")

        # Customer data 
        #print("\nPrint Customer sample data:")
        #df.show(5)

        return df

    except Exception as e:
        print(f"Error while loading Excel file: {e}")
        return None
    

def load_orders_data(spark, file_path):
    """Load orders JSON data using Spark."""

    # Define the orders schema
    orders_schema = StructType([
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

    print(f"\nChecking file at: {file_path}")

    if not os.path.exists(file_path):
        print("File not found!")
        return None

    print("File found. Loading JSON data using Spark...")

    try:
        # Read JSON file using Spark directly with defined schema
        # Use multiLine=True for JSON arrays
        df = spark.read.option("multiLine", "true").schema(orders_schema).json(file_path)

        print("Orders data loaded successfully")

        # Print schema
        #print("\nSchema:")
        #df.printSchema()

        # Record count
        #count = df.count()
        #print(f"\nTotal Records: {count}")

        # Order data 
        #print("\nPrint Orders sample data:")
        #df.show(5)

        return df

    except Exception as e:
        print(f"Error while loading JSON file: {e}")
        return None
    

def load_products_data(spark, file_path):
    """Load products CSV data using Spark."""

    # Define the product schema
    product_schema = StructType([
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", DoubleType(), True)
    ])

    print(f"\nChecking file at: {file_path}")

    if not os.path.exists(file_path):
        print("File not found!")
        return None

    print("File found. Loading CSV data using Spark...")

    try:
        # Read CSV file using Spark directly with defined schema
        df = spark.read.option("header", "true").schema(product_schema).csv(file_path)

        print("Products data loaded successfully")

        # Print schema
        #print("\nSchema:")
        #df.printSchema()

        # Record count
        #count = df.count()
        #print(f"\nTotal Records: {count}")

        # Products data 
        #print("\nPrint Products sample data:")
        #df.show(5)

        return df

    except Exception as e:
        print(f"Error while loading CSV file: {e}")
        return None


def perform_data_quality_checks(df, table_name):
    """
    Perform comprehensive data quality checks on a DataFrame.
    
    Parameters:
    - df: Spark DataFrame to check
    - table_name: Name of the table/dataset for reporting
    
    Returns:
    - Dictionary with quality check results
    """
    print(f"\n{'='*70}")
    print(f"DATA QUALITY REPORT: {table_name}")
    print(f"{'='*70}")
    
    quality_report = {
        "table_name": table_name,
        "dataframe_created": False,
        "columns_available": [],
        "total_columns": 0,
        "number_of_records": 0,
        "schema": None,
        "sample_records": [],
        "status": "Failed"
    }
    
    try:
        # Check 1: DataFrame is created
        if df is not None:
            quality_report["dataframe_created"] = True
            print(f"DataFrame Created: Yes")
        else:
            print(f"DataFrame Created: No (DataFrame is None)")
            return quality_report
        
        # Check 2: Columns available
        columns = df.columns
        quality_report["columns_available"] = columns
        print(f"Columns Available: {columns}")
        
        # Check 3: Total columns count
        total_cols = len(columns)
        quality_report["total_columns"] = total_cols
        print(f"Total Columns: {total_cols}")
        
        # Check 4: Number of records
        record_count = df.count()
        quality_report["number_of_records"] = record_count
        print(f"Number of Records: {record_count}")
        
        # Check 5: Table schema
        print(f"\nTable Schema:")
        df.printSchema()
        quality_report["schema"] = df.schema
        
        # Check 6: Sample records (2 records)
        print(f"\nSample Records (First 2 rows):")
        sample_data = df.take(2)
        quality_report["sample_records"] = sample_data
        df.show(2, truncate=False)
        
        # Mark as successful
        quality_report["status"] = "Passed"
        print(f"\n{'='*70}")
        print(f"DATA QUALITY CHECK FOR {table_name}: PASSED")
        print(f"{'='*70}\n")
        
        return quality_report
        
    except Exception as e:
        print(f"\nError during data quality checks: {e}")
        quality_report["error"] = str(e)
        print(f"\n{'='*70}")
        print(f"DATA QUALITY CHECK: FAILED")
        print(f"{'='*70}\n")
        return quality_report