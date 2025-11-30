"""
Data Cleaning Utilities Module

This module provides reusable data cleaning functions for orders, customers, and products data.
These functions implement data quality rules and can be imported and used across all modules
in the project to ensure consistent data cleaning across the codebase.

Data Quality Rules Applied:
Orders: Remove negative profits, NULL/duplicate Order IDs, invalid dates, NULL Customer/Product IDs
Customers: Remove NULL Customer IDs, duplicate Customer IDs
Products: Remove NULL Product IDs, duplicate Product IDs
"""

from pyspark.sql.functions import col, to_date


def clean_orders_for_enrichment(orders_df):
    """
    Cleans orders data before enrichment by applying data quality rules:
    Only applies rules if the required columns exist in the DataFrame.
    
    Orders: Remove negative profits, NULL/duplicate Order IDs, invalid dates, NULL Customer/Product IDs
    
    Parameters:
    - orders_df: Orders DataFrame to clean
    
    Returns:
    - Cleaned Orders DataFrame with quality metrics
    """
    print("\n--- Cleaning Orders Data for Enrichment ---")
    original_count = orders_df.count()
    
    # Get list of available columns
    available_columns = orders_df.columns
    
    # Rule 1: Remove records with negative Profit (only if column exists)
    if "Profit" in available_columns:
        print("Removing records with negative Profit...")
        negative_profit_count = orders_df.filter(col("Profit") < 0).count()
        print(f"   Records with negative Profit: {negative_profit_count}")
        orders_df = orders_df.filter(col("Profit") >= 0)
    else:
        print("Skipping negative Profit check (column not found)")
    
    # Rule 2: Remove records with NULL Order ID (only if column exists)
    if "Order ID" in available_columns:
        print("Removing records with NULL Order ID...")
        null_order_id_count = orders_df.filter(col("Order ID").isNull()).count()
        print(f"   Records with NULL Order ID: {null_order_id_count}")
        orders_df = orders_df.filter(col("Order ID").isNotNull())
        
        # Remove duplicate Order IDs (keep first occurrence)
        print("   Removing duplicate Order IDs...")
        duplicate_order_id_count = orders_df.count()
        orders_df = orders_df.dropDuplicates(["Order ID"])
        duplicate_order_id_count = duplicate_order_id_count - orders_df.count()
        print(f"   Records with duplicate Order ID removed: {duplicate_order_id_count}")
    else:
        print("Skipping Order ID validation (column not found)")
    
    # Rule 3: Validate Order Date and Ship Date formats (only if columns exist)
    if "Order Date" in available_columns and "Ship Date" in available_columns:
        print("Validating date formats...")
        df_with_valid_dates = orders_df.withColumn(
            "Order Date Parsed",
            to_date(col("Order Date"), "d/M/yyyy")
        ).withColumn(
            "Ship Date Parsed",
            to_date(col("Ship Date"), "d/M/yyyy")
        )
        
        invalid_date_count = df_with_valid_dates.filter(
            col("Order Date Parsed").isNull() | col("Ship Date Parsed").isNull()
        ).count()
        print(f"   Records with invalid date format: {invalid_date_count}")
        
        orders_df = df_with_valid_dates.filter(
            col("Order Date Parsed").isNotNull() & col("Ship Date Parsed").isNotNull()
        ).drop("Order Date Parsed", "Ship Date Parsed")
    else:
        print("Skipping date format validation (columns not found)")
    
    # Rule 4: Remove records with NULL Customer ID (only if column exists)
    if "Customer ID" in available_columns:
        print("Removing records with NULL Customer ID...")
        null_customer_id_count = orders_df.filter(col("Customer ID").isNull()).count()
        print(f"   Records with NULL Customer ID: {null_customer_id_count}")
        orders_df = orders_df.filter(col("Customer ID").isNotNull())
    else:
        print("Skipping Customer ID validation (column not found)")
    
    # Rule 5: Remove records with NULL Product ID (only if column exists)
    if "Product ID" in available_columns:
        print("Removing records with NULL Product ID...")
        null_product_id_count = orders_df.filter(col("Product ID").isNull()).count()
        print(f"   Records with NULL Product ID: {null_product_id_count}")
        orders_df = orders_df.filter(col("Product ID").isNotNull())
    else:
        print("Skipping Product ID validation (column not found)")
    
    final_count = orders_df.count()
    records_removed = original_count - final_count
    print(f"Orders data cleaning completed")
    print(f"  Original records: {original_count}, After cleaning: {final_count}")
    print(f"  Total records removed: {records_removed}")
    
    return orders_df


def clean_customers_for_enrichment(customers_df):
    """
    Cleans customers data before enrichment by applying data quality rules:
    Only applies rules if the required columns exist in the DataFrame.
    
    Remove records with NULL Customer ID and duplicate Customer IDs
    
    Parameters:
    - customers_df: Customers DataFrame to clean
    
    Returns:
    - Cleaned Customers DataFrame
    """
    print("\n--- Cleaning Customers Data for Enrichment ---")
    original_count = customers_df.count()
    
    # Get list of available columns
    available_columns = customers_df.columns
    
    # Rule 1: Remove records with NULL Customer ID (only if column exists)
    if "Customer ID" in available_columns:
        print("Removing records with NULL Customer ID...")
        null_customer_id_count = customers_df.filter(col("Customer ID").isNull()).count()
        print(f"   Records with NULL Customer ID: {null_customer_id_count}")
        customers_df = customers_df.filter(col("Customer ID").isNotNull())
        
        # Rule 2: Remove duplicate Customer IDs (keep first occurrence)
        print("Removing duplicate Customer IDs...")
        duplicate_customer_id_count = customers_df.count()
        customers_df = customers_df.dropDuplicates(["Customer ID"])
        duplicate_customer_id_count = duplicate_customer_id_count - customers_df.count()
        print(f"   Records with duplicate Customer ID removed: {duplicate_customer_id_count}")
    else:
        print("Skipping Customer ID validation (column not found)")
    
    final_count = customers_df.count()
    records_removed = original_count - final_count
    print(f"Customers data cleaning completed")
    print(f"  Original records: {original_count}, After cleaning: {final_count}")
    print(f"  Total records removed: {records_removed}")
    
    return customers_df


def clean_products_for_enrichment(products_df):
    """
    Cleans products data before enrichment by applying data quality rules:
    Only applies rules if the required columns exist in the DataFrame.
    
    Remove records with NULL Product ID and duplicate Product IDs
    
    Parameters:
    - products_df: Products DataFrame to clean
    
    Returns:
    - Cleaned Products DataFrame
    """
    print("\n--- Cleaning Products Data for Enrichment ---")
    original_count = products_df.count()
    
    # Get list of available columns
    available_columns = products_df.columns
    
    # Rule 1: Remove records with NULL Product ID (only if column exists)
    if "Product ID" in available_columns:
        print("Removing records with NULL Product ID...")
        null_product_id_count = products_df.filter(col("Product ID").isNull()).count()
        print(f"   Records with NULL Product ID: {null_product_id_count}")
        products_df = products_df.filter(col("Product ID").isNotNull())
        
        # Rule 2: Remove duplicate Product IDs (keep first occurrence)
        print("Removing duplicate Product IDs...")
        duplicate_product_id_count = products_df.count()
        products_df = products_df.dropDuplicates(["Product ID"])
        duplicate_product_id_count = duplicate_product_id_count - products_df.count()
        print(f"   Records with duplicate Product ID removed: {duplicate_product_id_count}")
    else:
        print("Skipping Product ID validation (column not found)")
    
    final_count = products_df.count()
    records_removed = original_count - final_count
    print(f"Products data cleaning completed")
    print(f"  Original records: {original_count}, After cleaning: {final_count}")
    print(f"  Total records removed: {records_removed}")
    
    return products_df
