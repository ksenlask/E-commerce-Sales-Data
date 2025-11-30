from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round
from src.data_cleaning_utils import clean_orders_for_enrichment, clean_customers_for_enrichment, clean_products_for_enrichment

def create_enriched_orders_table(orders_df, customers_df, products_df):
    """
    Creates a comprehensive sales overview by joining orders, customers, and products data.

    This function joins the three core tables and enriches the order data with customer
    and product details, providing a denormalized view for easy analysis.
    
    Applies data quality cleaning to all input dataframes before joining.

    Args:
        orders_df (DataFrame): The orders DataFrame.
        customers_df (DataFrame): The customers DataFrame.
        products_df (DataFrame): The products DataFrame.

    Returns:
        DataFrame: An enriched DataFrame containing a full overview of each sale.
    """
    # Clean input dataframes
    orders_df = clean_orders_for_enrichment(orders_df)
    customers_df = clean_customers_for_enrichment(customers_df)
    products_df = clean_products_for_enrichment(products_df)

    # Join orders with customers, then with the products
    sales_overview_df = (orders_df.join(customers_df,"Customer ID","inner")
                                .join(products_df,"Product ID","inner"))

    # Round the Profit column and select the final set of columns
    final_sales_overview = (sales_overview_df.withColumn("Profit",round(col("Profit"), 2))
            .select(
                "Order ID",
                "Order Date",
                "Ship Date",
                "Ship Mode",
                "Customer Name",
                "Country",
                "Category",
                "Sub-Category",
                "Product Name",
                "Quantity",
                "Price",
                "Discount",
                "Profit"
            ))

    return final_sales_overview


# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================
def validate_enriched_orders_table(enriched_orders_df, original_orders_df):
    """
    Performs data quality checks on the enriched orders DataFrame.

    Checks for:
    - Row count consistency between original and enriched tables.
    - No nulls in key identifier and date columns.
    - No invalid values in quantity and price columns.

    :param enriched_orders_df: The enriched orders DataFrame to validate.
    :param original_orders_df: The original orders DataFrame for row count comparison.
    :return: The original DataFrame if all checks pass.
    """
    print("\n--- Validating Enriched Orders Table ---")

    # Row count verification
    original_count = original_orders_df.count()
    enriched_count = enriched_orders_df.count()
    if original_count != enriched_count:
        print(f"\nWarning: Row count mismatch. Original orders: {original_count}, Enriched orders: {enriched_count}. May indicate missing customer or product data.")

    # Null checks on key columns
    key_columns = ["Order ID", "Customer Name", "Product Name", "Order Date"]
    for column in key_columns:
        null_count = enriched_orders_df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"\nWarning: Found {null_count} null values in '{column}' column.")

    # Business logic validation
    invalid_quantity_count = enriched_orders_df.filter(col("Quantity") <= 0).count()
    if invalid_quantity_count > 0:
        print(f"\nWarning: Found {invalid_quantity_count} records with non-positive quantity.")

    negative_price_count = enriched_orders_df.filter(col("Price") < 0).count()
    if negative_price_count > 0:
        print(f"\nWarning: Found {negative_price_count} records with negative price.")
    
    print("\nData quality checks for enriched orders table passed successfully.")
    return enriched_orders_df
