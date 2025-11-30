import pytest
from pyspark.sql.functions import col, to_date
from datetime import datetime, timedelta

from src.load_enriched_table import (
    summarize_customer_spending,
    calculate_average_basket_size,
    measure_customer_product_variety,
    identify_at_risk_customers,
    classify_product_price_level,
    identify_fast_and_slow_sellers,
    validate_enriched_customer_data,
    validate_enriched_product_data
)

# ============================================================================
# TESTS FOR CUSTOMER ENRICHMENT
# ============================================================================

class TestCustomerEnrichment:
    """Test cases for customer enrichment functions."""

    def test_summarize_customer_spending_calculates_correctly(self, spark):
        """
        Tests that total_sales, total_profit, and total_orders are calculated correctly.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        orders = spark.createDataFrame([
            ("O1", "C1", "P1", 100, 20, 1),
            ("O2", "C1", "P2", 50, 10, 1),
            ("O3", "C1", "P3", 100, 20, 1)
        ], ["Order ID", "Customer ID", "Product ID", "Price", "Profit", "Quantity"])

        result_df = summarize_customer_spending(customers, orders)
        result = result_df.first()

        assert result["total_sales"] == 250
        assert result["total_profit"] == 50
        assert result["total_orders"] == 3

    def test_summarize_customer_spending_handles_no_orders(self, spark):
        """
        Tests that customers with no orders have null or zero values.
        """
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        
        # Define the schema explicitly for an empty orders DataFrame
        orders_schema = StructType([
            StructField("Order ID", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Product ID", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Profit", DoubleType(), True),
            StructField("Quantity", IntegerType(), True)
        ])
        orders = spark.createDataFrame([], schema=orders_schema) # Empty orders df

        result_df = summarize_customer_spending(customers, orders)
        result = result_df.first()

        assert result["total_sales"] is None
        assert result["total_profit"] is None
        assert result["total_orders"] is None

    def test_calculate_average_basket_size_calculates_correctly(self, spark):
        """
        Tests if the average_order_value is computed correctly.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        orders = spark.createDataFrame([
            ("O1", "C1", 100),
            ("O2", "C1", 50),
        ], ["Order ID", "Customer ID", "Price"])

        result_df = calculate_average_basket_size(customers, orders)
        result = result_df.first()

        assert result["average_order_value"] == 75.0

    def test_measure_customer_product_variety_counts_distinct_products(self, spark):
        """
        Tests that unique_products_purchased correctly counts distinct products.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        orders = spark.createDataFrame([
            ("C1", "P1"),
            ("C1", "P2"),
            ("C1", "P1"), # Duplicate product
        ], ["Customer ID", "Product ID"])

        result_df = measure_customer_product_variety(customers, orders)
        result = result_df.first()

        assert result["unique_products_purchased"] == 2

    def test_identify_at_risk_customers_low_risk(self, spark):
        """
        Tests that a customer with a recent purchase is labeled 'Low Risk'.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        today = datetime.now()
        orders = spark.createDataFrame([
            ("C1", today - timedelta(days=30)),
        ], ["Customer ID", "Order Date"])

        result_df = identify_at_risk_customers(customers, orders)
        result = result_df.first()

        assert result["churn_risk_score"] == "Low Risk"
        assert result["days_since_last_order"] == 30

    def test_identify_at_risk_customers_medium_risk(self, spark):
        """
        Tests that a customer is labeled 'Medium Risk'.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        today = datetime.now()
        orders = spark.createDataFrame([
            ("C1", today - timedelta(days=100)),
        ], ["Customer ID", "Order Date"])

        result_df = identify_at_risk_customers(customers, orders)
        result = result_df.first()

        assert result["churn_risk_score"] == "Medium Risk"

    def test_identify_at_risk_customers_high_risk(self, spark):
        """
        Tests that a customer is labeled 'High Risk'.
        """
        customers = spark.createDataFrame([("C1", "Kushal")], ["Customer ID", "Customer Name"])
        today = datetime.now()
        orders = spark.createDataFrame([
            ("C1", today - timedelta(days=200)),
        ], ["Customer ID", "Order Date"])

        result_df = identify_at_risk_customers(customers, orders)
        result = result_df.first()

        assert result["churn_risk_score"] == "High Risk"

# ============================================================================
# TESTS FOR PRODUCT ENRICHMENT
# ============================================================================

class TestProductEnrichment:
    """Test cases for product enrichment functions."""

    def test_classify_product_price_level_premium(self, spark):
        """
        Tests that a product is correctly classified as 'Premium'.
        """
        products = spark.createDataFrame([
            ("P1", "Electronics", 1200),
            ("P2", "Electronics", 1000),
            ("P3", "Electronics", 200),
            ("P4", "Electronics", 100),
        ], ["Product ID", "Category", "Price per product"])

        result_df = classify_product_price_level(products)
        premium_product = result_df.where(col("Product ID") == "P1").first()

        assert premium_product["price_position"] == "Premium"

    def test_classify_product_price_level_mid_range(self, spark):
        """
        Tests that a product is correctly classified as 'Mid-Range'.
        """
        products = spark.createDataFrame([
            ("P1", "Electronics", 1200), # p75
            ("P2", "Electronics", 800),  # Mid-Range
            ("P3", "Electronics", 400),  # p25
            ("P4", "Electronics", 100),  # Budget
        ], ["Product ID", "Category", "Price per product"])

        result_df = classify_product_price_level(products)
        mid_product = result_df.where(col("Product ID") == "P2").first()

        assert mid_product["price_position"] == "Mid-Range"

    def test_classify_product_price_level_budget(self, spark):
        """
        Tests that a product is correctly classified as 'Budget'.
        """
        products = spark.createDataFrame([
            ("P1", "Electronics", 1200), # p75
            ("P2", "Electronics", 800),  # Mid-Range
            ("P3", "Electronics", 400),  # p25
            ("P4", "Electronics", 100),  # Budget
        ], ["Product ID", "Category", "Price per product"])

        result_df = classify_product_price_level(products)
        budget_product = result_df.where(col("Product ID") == "P4").first()

        assert budget_product["price_position"] == "Budget"

    def test_identify_fast_and_slow_sellers_fast_moving(self, spark):
        """
        Tests that a product is correctly classified as 'Fast Moving'.
        """
        products = spark.createDataFrame([("P1", "Laptop")], ["Product ID", "Product Name"])
        orders = spark.createDataFrame([("P1", 150)], ["Product ID", "Quantity"])

        result_df = identify_fast_and_slow_sellers(products, orders)
        result = result_df.first()

        assert result["inventory_velocity"] == "Fast Moving"
        assert result["total_quantity_sold"] == 150

    def test_identify_fast_and_slow_sellers_medium_moving(self, spark):
        """
        Tests that a product is correctly classified as 'Medium Moving'.
        """
        products = spark.createDataFrame([("P1", "Mouse")], ["Product ID", "Product Name"])
        orders = spark.createDataFrame([("P1", 50)], ["Product ID", "Quantity"])

        result_df = identify_fast_and_slow_sellers(products, orders)
        result = result_df.first()

        assert result["inventory_velocity"] == "Medium Moving"

    def test_identify_fast_and_slow_sellers_slow_moving(self, spark):
        """
        Tests that a product is correctly classified as 'Slow Moving'.
        """
        products = spark.createDataFrame([("P1", "Webcam")], ["Product ID", "Product Name"])
        orders = spark.createDataFrame([("P1", 10)], ["Product ID", "Quantity"])

        result_df = identify_fast_and_slow_sellers(products, orders)
        result = result_df.first()

        assert result["inventory_velocity"] == "Slow Moving"



# ============================================================================
# TESTS FOR DATA VALIDATION
# ============================================================================

class TestDataValidation:
    """Test cases for data validation functions."""

    def test_validate_enriched_customer_data_handles_null_ids(self, spark, capsys):
        """
        Tests that the validation function catches null Customer IDs and prints a warning.
        """
        # Create a DataFrame with a null Customer ID
        df = spark.createDataFrame([
            ("C1", 100.0, 20.0, 2, 50.0, 1, "Low Risk", 30),
            (None, 200.0, 40.0, 3, 66.6, 2, "Medium Risk", 100)
        ], ["Customer ID", "total_sales", "total_profit", "total_orders", "average_order_value", "unique_products_purchased", "churn_risk_score", "days_since_last_order"])

        validate_enriched_customer_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 null 'Customer ID' entries." in captured.out

    def test_validate_enriched_customer_data_handles_duplicate_ids(self, spark, capsys):
        """
        Tests that the validation function catches duplicate Customer IDs and prints a warning.
        """
        df = spark.createDataFrame([
            ("C1", 100.0, 20.0, 2, 50.0, 1, "Low Risk", 30),
            ("C1", 200.0, 40.0, 3, 66.6, 2, "Medium Risk", 100)
        ], ["Customer ID", "total_sales", "total_profit", "total_orders", "average_order_value", "unique_products_purchased", "churn_risk_score", "days_since_last_order"])

        validate_enriched_customer_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 duplicate 'Customer ID' entries." in captured.out

    def test_validate_enriched_customer_data_handles_negative_sales(self, spark, capsys):
        """
        Tests that the validation function catches negative total_sales and prints a warning.
        """
        df = spark.createDataFrame([
            ("C1", -100.0, 20.0, 2, 50.0, 1, "Low Risk", 30)
        ], ["Customer ID", "total_sales", "total_profit", "total_orders", "average_order_value", "unique_products_purchased", "churn_risk_score", "days_since_last_order"])

        validate_enriched_customer_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 records with negative 'total_sales' or 'total_profit'." in captured.out

    def test_validate_enriched_product_data_handles_null_ids(self, spark, capsys):
        """
        Tests that the validation function catches null Product IDs and prints a warning.
        """
        df = spark.createDataFrame([
            ("P1", "Premium", "Fast Moving", 150, 0.0),
            (None, "Budget", "Slow Moving", 10, 0.0)
        ], ["Product ID", "price_position", "inventory_velocity", "total_quantity_sold", "return_rate_percent"])

        validate_enriched_product_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 null 'Product ID' entries." in captured.out

    def test_validate_enriched_product_data_handles_duplicate_ids(self, spark, capsys):
        """
        Tests that the validation function catches duplicate Product IDs and prints a warning.
        """
        df = spark.createDataFrame([
            ("P1", "Premium", "Fast Moving", 150, 0.0),
            ("P1", "Budget", "Slow Moving", 10, 0.0)
        ], ["Product ID", "price_position", "inventory_velocity", "total_quantity_sold", "return_rate_percent"])

        validate_enriched_product_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 duplicate 'Product ID' entries." in captured.out

    def test_validate_enriched_product_data_handles_negative_quantity(self, spark, capsys):
        """
        Tests that the validation function catches negative total_quantity_sold and prints a warning.
        """
        df = spark.createDataFrame([
            ("P1", "Premium", "Fast Moving", -150, 0.0)
        ], ["Product ID", "price_position", "inventory_velocity", "total_quantity_sold", "return_rate_percent"])

        validate_enriched_product_data(df)
        captured = capsys.readouterr()
        assert "\nWarning: Found 1 records with negative 'total_quantity_sold'." in captured.out
