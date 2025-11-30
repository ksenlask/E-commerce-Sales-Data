from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count, year, round, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# ============================================================================
# PROFIT SUMMARY CREATION
# ============================================================================

def create_profit_summary_table(enriched_orders_df):
    """
    Creates a profit summary table by aggregating profit across multiple dimensions:
    Year, Product Category, Product Sub-Category, and Customer.

    This function provides a comprehensive view of profit distribution across these
    key business dimensions, enabling analysis of profitability by time period,
    product lines, and customer segments.
    
    NOTE: This function expects to receive an already-cleaned and enriched DataFrame
    from create_enriched_orders_table(), which includes the application of data quality rules.
    This function aggregates all profits, including negative values (losses), as they are
    important for complete financial analysis.

    Args:
        enriched_orders_df (DataFrame): The enriched orders DataFrame containing
                                        order date, category, sub-category, 
                                        customer name, and profit columns.

    Returns:
        DataFrame: An aggregated DataFrame with the following columns:
                   - Year: Extracted from Order Date
                   - Category: Product Category
                   - Sub-Category: Product Sub-Category
                   - Customer Name: Name of the customer
                   - Total_Profit: Sum of profit for the group
                   - Order_Count: Number of orders in the group
    """
    # Perform minimal data cleaning for date validation only
    # (Keep this function focused on aggregation, not on removing negative profits)
    
    # Convert Order Date to date type (handles string format from source data)
    # Try to parse dates; if they fail to parse, they'll be NULL
    enriched_with_date = enriched_orders_df.withColumn(
        "Order_Date_Parsed",
        to_date(col("Order Date"), "d/M/yyyy")
    )
    
    # Extract year from Order Date and create the summary
    profit_summary_df = (enriched_with_date
                        .withColumn("Year", year(col("Order_Date_Parsed")))
                        .groupBy("Year", "Category", "Sub-Category", "Customer Name")
                        .agg(
                            round(sum("Profit"), 2).alias("Total_Profit"),
                            count("Order ID").alias("Order_Count")
                        )
                        .orderBy("Year", "Category", "Sub-Category", "Customer Name"))

    return profit_summary_df


# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

def validate_profit_summary_table(profit_summary_df, original_enriched_orders_df):
    """
    Performs data quality checks on the profit summary DataFrame.

    Checks for:
    - No nulls in critical identifier columns (Year, Category, Sub-Category, Customer Name).
    - No duplicate records (combinations of Year, Category, Sub-Category, Customer Name).
    - All financial values (Total_Profit) are present and valid.
    - Order_Count is a positive integer.
    - Row count consistency: profit summary rows should be less than or equal to original orders.

    Args:
        profit_summary_df (DataFrame): The profit summary DataFrame to validate.
        original_enriched_orders_df (DataFrame): The original enriched orders DataFrame for reference.

    Returns:
        DataFrame: The original profit_summary_df if all checks pass.
    """
    print("\n--- Validating Profit Summary Table ---")

    # Check for nulls in critical columns
    key_columns = ["Year", "Category", "Sub-Category", "Customer Name"]
    for column in key_columns:
        null_count = profit_summary_df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"\nWarning: Found {null_count} null values in '{column}' column.")

    # Check for duplicate records
    total_rows = profit_summary_df.count()
    distinct_combinations = profit_summary_df.dropDuplicates(key_columns).count()
    
    if total_rows != distinct_combinations:
        print(f"\nWarning: Found {total_rows - distinct_combinations} duplicate records.")
    else:
        print(f"\nInfo: All {total_rows} records are unique combinations.")

    # Check for null or invalid Total_Profit values
    null_profit_count = profit_summary_df.filter(col("Total_Profit").isNull()).count()
    if null_profit_count > 0:
        print(f"\nWarning: Found {null_profit_count} null 'Total_Profit' values.")

    # Check for invalid Order_Count (should be positive integer)
    invalid_order_count = profit_summary_df.filter(col("Order_Count") <= 0).count()
    if invalid_order_count > 0:
        print(f"\nWarning: Found {invalid_order_count} records with non-positive 'Order_Count'.")

    # Row count sanity check
    original_order_count = original_enriched_orders_df.count()
    summary_row_count = profit_summary_df.count()
    
    if summary_row_count > original_order_count:
        print(f"\nWarning: Profit summary row count ({summary_row_count}) exceeds original orders ({original_order_count}).")
    else:
        print(f"\nInfo: Profit summary has {summary_row_count} unique groups from {original_order_count} orders.")

    print("\nData quality checks for profit summary table passed successfully.")
    return profit_summary_df
