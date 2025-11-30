#!/usr/bin/env python3

import os
from spark_session import get_spark_session
from load_source_data import load_customer_data, load_orders_data, load_products_data, perform_data_quality_checks
from load_enriched_table import (
    summarize_customer_spending,
    calculate_average_basket_size,
    measure_customer_product_variety,
    identify_at_risk_customers,
    classify_product_price_level,
    identify_fast_and_slow_sellers,
    validate_enriched_customer_data,
    validate_enriched_product_data
)
from load_enriched_orders import (
    create_enriched_orders_table,
    validate_enriched_orders_table,
)
from load_profit_summary import (
    create_profit_summary_table,
    validate_profit_summary_table,
)
from sql_profit_aggregates import (
    get_all_profit_aggregates,
    validate_all_aggregates,
)

from pyspark.sql.types import StructType, StructField, StringType

def main():

    # Create Spark session (only needed when running outside Databricks)
    spark = get_spark_session("CustomerDataLoader")
    
    # ============================================================================
    # DATA LOADING
    # ============================================================================
    print("\n=== Data Loading Started ===")
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    customers_df = load_customer_data(spark, os.path.join(project_root, "data", "Customer.xlsx"))
    orders_df = load_orders_data(spark, os.path.join(project_root, "data", "Orders.json"))
    products_df = load_products_data(spark, os.path.join(project_root, "data", "Products.csv"))
    
    print("--- Customer, Orders, and Products data loaded successfully ---")
    print("=== Data Loading Completed ===")

    # ============================================================================
    # DATA QUALITY CHECKS
    # ============================================================================
    print("\n=== Data Quality Checks Started ===")
    
    perform_data_quality_checks(products_df, "Products")    
    perform_data_quality_checks(customers_df, "Customers")
    perform_data_quality_checks(orders_df, "Orders")
    
    print("--- Data quality checks completed for all tables ---")
    print("=== Data Quality Checks Completed ===")
    
    # ============================================================================
    # DATA ENRICHMENT
    # ============================================================================
    
    print("\n=== Data Enrichment Started ===")

    # Customer Enrichments
    print("\n--- Applying Customer Enrichments ---")
    enriched_customers_df = summarize_customer_spending(customers_df, orders_df)
    enriched_customers_df = calculate_average_basket_size(enriched_customers_df, orders_df)
    enriched_customers_df = measure_customer_product_variety(enriched_customers_df, orders_df)
    enriched_customers_df = identify_at_risk_customers(enriched_customers_df, orders_df)

    # Product Enrichments
    print("\n--- Applying Product Enrichments ---")
    enriched_products_df = classify_product_price_level(products_df)
    enriched_products_df = identify_fast_and_slow_sellers(enriched_products_df, orders_df)

    # Validate enriched data
    print("\n--- Validating Enriched Data ---")
    enriched_customers_df = validate_enriched_customer_data(enriched_customers_df)
    enriched_products_df = validate_enriched_product_data(enriched_products_df)

    # Show the enriched dataframes
    print("\n--- Enriched Customers Data ---")
    enriched_customers_df.show(2)

    print("\n--- Enriched Products Data ---")
    enriched_products_df.show(2)

    print("\n=== Data Enrichment Completed ===")

    # ============================================================================
    # SALES OVERVIEW
    # ============================================================================
    print("\n=== Sales Overview Started ===")

    # Create the enriched orders table
    enriched_orders_df = create_enriched_orders_table(orders_df, customers_df, products_df)
    
    # Validate the enriched orders table
    enriched_orders_df = validate_enriched_orders_table(enriched_orders_df, orders_df)

    # Show the new dataframes
    print("\n--- Enriched Orders Data (Sales Overview) ---")
    enriched_orders_df.filter("Profit >= 0").show(5)

    print("\n=== Sales Overview Completed ===")

    # ============================================================================
    # PROFIT SUMMARY
    # ============================================================================
    print("\n=== Profit Summary Started ===")

    # Create profit summary table
    profit_summary_df = create_profit_summary_table(enriched_orders_df)
    
    # Validate the profit summary table
    profit_summary_df = validate_profit_summary_table(profit_summary_df, enriched_orders_df)

    # Show the profit summary table
    print("\n--- Profit Summary Table (Year, Category, Sub-Category, Customer) ---")
    profit_summary_df.show(10)

    print("\n=== Profit Summary Completed ===")

    # ============================================================================
    # SQL PROFIT AGGREGATES
    # ============================================================================
    print("\n=== SQL Profit Aggregates Started ===")

    # Generate all four profit aggregates
    aggregates = get_all_profit_aggregates(enriched_orders_df)

    # Display aggregates
    print("\n--- Profit by Year ---")
    aggregates['profit_by_year'].show()

    print("\n--- Profit by Year + Category ---")
    aggregates['profit_by_year_category'].show()

    print("\n--- Profit by Customer (Top 10) ---")
    aggregates['profit_by_customer'].limit(10).show()

    print("\n--- Profit by Customer + Year (Sample) ---")
    aggregates['profit_by_customer_year'].limit(10).show()

    # Validate aggregates
    print("\n--- Validating Aggregates ---")
    validation_results = validate_all_aggregates(aggregates)

    print("\n=== SQL Profit Aggregates Completed ===")


    print("\nStopping Spark session.")
    spark.stop()


if __name__ == "__main__":
    main()
