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
# BATCH AGGREGATES FUNCTION
# ============================================================================

def get_all_profit_aggregates(enriched_orders_df):
    return {
        'profit_by_year': get_profit_by_year(enriched_orders_df),
        'profit_by_year_category': get_profit_by_year_category(enriched_orders_df),
        'profit_by_customer': get_profit_by_customer(enriched_orders_df),
        'profit_by_customer_year': get_profit_by_customer_year(enriched_orders_df)
    }


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_aggregate_output(aggregate_df, aggregate_type):
    print(f"\n--- Validating {aggregate_type} ---")
    
    # Check if DataFrame is empty
    if aggregate_df.count() == 0:
        print(f"Warning: {aggregate_type} returned no results")
        return False
    
    # Check for NULL values in Profit column
    null_profit = aggregate_df.filter("Total_Profit IS NULL").count()
    if null_profit > 0:
        print(f"Warning: Found {null_profit} NULL values in Total_Profit")
        return False
    
    # Check for NULL values in Order_Count
    null_count = aggregate_df.filter("Order_Count IS NULL").count()
    if null_count > 0:
        print(f"Warning: Found {null_count} NULL values in Order_Count")
        return False
    
    # Check for negative profits
    negative_profit = aggregate_df.filter("Total_Profit < 0").count()
    if negative_profit > 0:
        print(f"Warning: Found {negative_profit} rows with negative Total_Profit")
        return False
    
    # Check for zero or negative order counts
    invalid_count = aggregate_df.filter("Order_Count <= 0").count()
    if invalid_count > 0:
        print(f"Warning: Found {invalid_count} rows with Order_Count <= 0")
        return False
    
    print(f"{aggregate_type} validation passed ({aggregate_df.count()} rows)")
    return True


def validate_all_aggregates(aggregates):
    results = {}
    for name, df in aggregates.items():
        # Convert name from key format to display format
        display_name = name.replace('_', ' ').title()
        results[name] = validate_aggregate_output(df, display_name)
    
    return results
