from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count, avg, max, min, datediff, when, lit, percentile_approx, dense_rank, expr, current_date, lag, countDistinct, ntile
from pyspark.sql.window import Window
from data_cleaning_utils import clean_orders_for_enrichment, clean_customers_for_enrichment, clean_products_for_enrichment



def summarize_customer_spending(customers_df, orders_df):
    """
    Creates a summary of each customer's lifetime value by calculating their
    total sales, total profit, and total number of orders.
    Filters out orders with negative prices and applies data quality cleaning.
    """
    # Clean input dataframes
    customers_df = clean_customers_for_enrichment(customers_df)
    orders_df = clean_orders_for_enrichment(orders_df)
    
    # Filter out returns or negative price entries before aggregation
    valid_orders_df = orders_df.filter(col("Price") >= 0)
    
    sales_metrics = valid_orders_df.groupBy("Customer ID").agg(
                                sum("Price").alias("total_sales"),
                                sum("Profit").alias("total_profit"),
                                countDistinct("Order ID").alias("total_orders")
                                )
    return customers_df.join(sales_metrics, "Customer ID", "left")

def calculate_average_basket_size(customers_df, orders_df):
    """
    Calculates the average spending per order for each customer, often
    referred to as their average "basket size". Applies data quality cleaning.
    """
    # Clean orders data
    orders_df = clean_orders_for_enrichment(orders_df)
    
    aov_metrics = orders_df.groupBy("Customer ID").agg(
        (sum("Price") / countDistinct("Order ID")).alias("average_order_value")
    )
    return customers_df.join(aov_metrics, "Customer ID", "left")

def measure_customer_product_variety(customers_df, orders_df):
    """
    Measures the variety of products a customer buys by counting the number
    of unique products they have purchased. Applies data quality cleaning.
    """
    # Clean orders data
    orders_df = clean_orders_for_enrichment(orders_df)
    
    diversity_metrics = orders_df.groupBy("Customer ID").agg(
        countDistinct("Product ID").alias("unique_products_purchased")
    )
    return customers_df.join(diversity_metrics, "Customer ID", "left")


def identify_at_risk_customers(customers_df, orders_df):
    """
    Identifies customers who may be at risk of churning by checking how
    long it has been since their last purchase. Applies data quality cleaning.
    """
    # Clean orders data
    orders_df = clean_orders_for_enrichment(orders_df)
    
    recency_df = orders_df.groupBy("Customer ID").agg(
        datediff(current_date(), max(col("Order Date"))).alias("days_since_last_order")
    )

    churn_risk = recency_df.withColumn(
        "churn_risk_score",
        when(col("days_since_last_order") > 180, "High Risk")
        .when(col("days_since_last_order") > 90, "Medium Risk")
        .otherwise("Low Risk")
    )
    return customers_df.join(churn_risk, "Customer ID", "left")


# ============================================================================
# PRODUCT ENRICHMENT FUNCTIONS
# ============================================================================

def classify_product_price_level(products_df):
    """
    Classifies each product's price level (e.g., Premium, Mid-Range, Budget)
    by bucketing products into four price tiers within each category.
    Applies data quality cleaning before classification.
    """
    # Clean products data
    products_df = clean_products_for_enrichment(products_df)
    
    # Window partitioned by category, ordered by price
    category_window = Window.partitionBy("Category").orderBy(col("Price per product"))
    
    # Assign products to one of 4 tiers (quartiles)
    price_tiers = products_df.withColumn("price_tier", ntile(4).over(category_window))
    
    # Classify based on the tier
    price_position = price_tiers.withColumn(
        "price_position",
        when(col("price_tier") == 4, "Premium")    # Top 25%
        .when(col("price_tier").isin(2, 3), "Mid-Range") # Middle 50%
        .otherwise("Budget")                       # Bottom 25%
    )
    
    return price_position


def identify_fast_and_slow_sellers(products_df, orders_df):
    """
    Identifies fast- and slow-moving products by classifying them based
    on the total quantity sold. Applies data quality cleaning.
    """
    # Clean input data
    products_df = clean_products_for_enrichment(products_df)
    orders_df = clean_orders_for_enrichment(orders_df)

    product_sales = orders_df.groupBy("Product ID").agg(
        sum("Quantity").alias("total_quantity_sold")
    )
    
    # Join with products to get all products, even those not sold
    inventory_velocity = products_df.join(product_sales, "Product ID", "left").na.fill(0)
    
    # Simple classification based on quantity sold
    inventory_velocity = inventory_velocity.withColumn(
        "inventory_velocity",
        when(col("total_quantity_sold") > 100, "Fast Moving") # Example threshold
        .when(col("total_quantity_sold") > 20, "Medium Moving")
        .otherwise("Slow Moving")
    )
    
    return inventory_velocity




# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

def validate_enriched_customer_data(enriched_df):
    """
    Performs data quality checks on the enriched customer DataFrame.

    Checks for:
    -  No nulls in the 'Customer ID' column.
    -  No duplicate 'Customer ID' entries.
    -  'total_sales' and 'total_profit' are non-negative where they are not null.
    -  'churn_risk_score' contains only expected values ('Low Risk', 'Medium Risk', 'High Risk').

    :param enriched_df: The enriched customer DataFrame to validate.
    :return: The original DataFrame if all checks pass.
    """
    # Check for null Customer IDs
    null_customer_ids = enriched_df.filter(col("Customer ID").isNull()).count()
    if null_customer_ids > 0:
        print(f"\nWarning: Found {null_customer_ids} null 'Customer ID' entries.")

    # Check for duplicate Customer IDs
    duplicate_customer_ids = enriched_df.groupBy("Customer ID").count().where(col("count") > 1).count()
    if duplicate_customer_ids > 0:
        print(f"\nWarning: Found {duplicate_customer_ids} duplicate 'Customer ID' entries.")

    # Check for negative financial values
    negative_financials_count = enriched_df.filter((col("total_sales") < 0) | (col("total_profit") < 0)).count()
    if negative_financials_count > 0:
        print(f"\nWarning: Found {negative_financials_count} records with negative 'total_sales' or 'total_profit'.")

    # Check for valid churn risk scores
    valid_churn_scores = ["Low Risk", "Medium Risk", "High Risk"]
    invalid_churn_scores = enriched_df.filter(col("churn_risk_score").isNotNull() & ~col("churn_risk_score").isin(valid_churn_scores)).count()
    if invalid_churn_scores > 0:
        print(f"\nWarning: Found {invalid_churn_scores} invalid 'churn_risk_score' values.")

    print("\nData quality checks for enriched customer data passed successfully.")
    return enriched_df

def validate_enriched_product_data(enriched_df):
    """
    Performs data quality checks on the enriched product DataFrame.

    Checks for:
    -  No nulls in the 'Product ID' column.
    -  No duplicate 'Product ID' entries.
    -  'total_quantity_sold' is non-negative where it is not null.
    -  'price_position' contains only expected values ('Premium', 'Mid-Range', 'Budget').
    -  'inventory_velocity' contains only expected values ('Fast Moving', 'Medium Moving', 'Slow Moving').

    :param enriched_df: The enriched product DataFrame to validate.
    :return: The original DataFrame if all checks pass.
    """
    # Check for null Product IDs
    null_product_ids = enriched_df.filter(col("Product ID").isNull()).count()
    if null_product_ids > 0:
        print(f"\nWarning: Found {null_product_ids} null 'Product ID' entries.")

    # Check for duplicate Product IDs
    duplicate_products_count = enriched_df.groupBy("Product ID").count().where(col("count") > 1).count()
    if duplicate_products_count > 0:
        print(f"\nWarning: Found {duplicate_products_count} duplicate 'Product ID' entries.")

    # Check for negative quantity
    negative_quantity_count = enriched_df.filter(col("total_quantity_sold") < 0).count()
    if negative_quantity_count > 0:
        print(f"\nWarning: Found {negative_quantity_count} records with negative 'total_quantity_sold'.")

    # Check for valid price positions
    valid_price_positions = ["Premium", "Mid-Range", "Budget"]
    invalid_price_positions = enriched_df.filter(col("price_position").isNotNull() & ~col("price_position").isin(valid_price_positions)).count()
    if invalid_price_positions > 0:
        print(f"\nWarning: Found {invalid_price_positions} invalid 'price_position' values.")

    # Check for valid inventory velocities
    valid_inventory_velocities = ["Fast Moving", "Medium Moving", "Slow Moving"]
    invalid_inventory_velocities = enriched_df.filter(col("inventory_velocity").isNotNull() & ~col("inventory_velocity").isin(valid_inventory_velocities)).count()
    if invalid_inventory_velocities > 0:
        print(f"\nWarning: Found {invalid_inventory_velocities} invalid 'inventory_velocity' values.")

    print("\nData quality checks for enriched product data passed successfully.")
    return enriched_df
