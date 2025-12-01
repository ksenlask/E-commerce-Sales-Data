from pyspark.sql import DataFrame


# ============================================================================
# PROFIT BY YEAR
# ============================================================================

def get_profit_by_year(enriched_orders_df):
    enriched_orders_df.createOrReplaceTempView("enriched_orders")
    
    query = """
    SELECT 
        YEAR(TO_DATE(`Order Date`, 'd/M/yyyy')) AS Year,
        ROUND(SUM(Profit), 2) AS Total_Profit,
        COUNT(DISTINCT `Order ID`) AS Order_Count
    FROM enriched_orders
    GROUP BY YEAR(TO_DATE(`Order Date`, 'd/M/yyyy'))
    ORDER BY Year
    """
    
    result_df = enriched_orders_df.sparkSession.sql(query)
    return result_df


# ============================================================================
# PROFIT BY YEAR + PRODUCT CATEGORY
# ============================================================================

def get_profit_by_year_category(enriched_orders_df):
    enriched_orders_df.createOrReplaceTempView("enriched_orders")
    
    query = """
    SELECT 
        YEAR(TO_DATE(`Order Date`, 'd/M/yyyy')) AS Year,
        Category,
        ROUND(SUM(Profit), 2) AS Total_Profit,
        COUNT(DISTINCT `Order ID`) AS Order_Count
    FROM enriched_orders
    GROUP BY YEAR(TO_DATE(`Order Date`, 'd/M/yyyy')), Category
    ORDER BY Year, Category
    """
    
    result_df = enriched_orders_df.sparkSession.sql(query)
    return result_df


# ============================================================================
# PROFIT BY CUSTOMER
# ============================================================================

def get_profit_by_customer(enriched_orders_df):
    enriched_orders_df.createOrReplaceTempView("enriched_orders")
    
    query = """
    SELECT 
        `Customer Name`,
        ROUND(SUM(Profit), 2) AS Total_Profit,
        COUNT(DISTINCT `Order ID`) AS Order_Count
    FROM enriched_orders
    GROUP BY `Customer Name`
    ORDER BY Total_Profit DESC
    """
    
    result_df = enriched_orders_df.sparkSession.sql(query)
    return result_df


# ============================================================================
# PROFIT BY CUSTOMER + YEAR
# ============================================================================

def get_profit_by_customer_year(enriched_orders_df):
    enriched_orders_df.createOrReplaceTempView("enriched_orders")
    
    query = """
    SELECT 
        `Customer Name`,
        YEAR(TO_DATE(`Order Date`, 'd/M/yyyy')) AS Year,
        ROUND(SUM(Profit), 2) AS Total_Profit,
        COUNT(DISTINCT `Order ID`) AS Order_Count
    FROM enriched_orders
    GROUP BY `Customer Name`, YEAR(TO_DATE(`Order Date`, 'd/M/yyyy'))
    ORDER BY Year, `Customer Name`
    """
    
    result_df = enriched_orders_df.sparkSession.sql(query)
    return result_df


# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

def validate_profit_aggregates(aggregate_df, aggregate_name):
    """
    Validates profit aggregate DataFrames for data quality.
    
    Checks for:
    - Non-empty results
    - No NULL values in Total_Profit or Order_Count
    - Positive Order_Count values
    """
    print(f"\n--- Validating {aggregate_name} ---")
    
    row_count = aggregate_df.count()
    if row_count == 0:
        print(f"Warning: {aggregate_name} returned no results")
        return False
    
    null_profit = aggregate_df.filter("Total_Profit IS NULL").count()
    if null_profit > 0:
        print(f"Warning: Found {null_profit} NULL values in Total_Profit")
    
    null_count = aggregate_df.filter("Order_Count IS NULL").count()
    if null_count > 0:
        print(f"Warning: Found {null_count} NULL values in Order_Count")
    
    invalid_count = aggregate_df.filter("Order_Count <= 0").count()
    if invalid_count > 0:
        print(f"Warning: Found {invalid_count} rows with Order_Count <= 0")
    
    print(f"{aggregate_name} validation passed ({row_count} rows)")
    return True
