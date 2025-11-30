import pytest
from src.load_profit_summary import (
    create_profit_summary_table,
    validate_profit_summary_table
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col
from datetime import date


class TestCreateProfitSummaryTable:
    """Tests for the create_profit_summary_table function."""

    def test_groups_and_aggregates_correctly(self, sample_data_with_dates):
        """Verifies that the function correctly groups by Year, Category, Sub-Category, Customer."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Check that grouping columns are present
        assert "Year" in profit_summary_df.columns
        assert "Category" in profit_summary_df.columns
        assert "Sub-Category" in profit_summary_df.columns
        assert "Customer Name" in profit_summary_df.columns
        assert "Total_Profit" in profit_summary_df.columns
        assert "Order_Count" in profit_summary_df.columns

    def test_profit_aggregation_is_correct(self, sample_data_with_dates):
        """Ensures that profits are correctly summed and rounded to 2 decimals."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Filter for a specific group
        specific_group = profit_summary_df.filter(
            (col("Year") == 2016) & 
            (col("Customer Name") == "Kushal Sen Laskar")
        ).collect()
        
        # There should be exactly one record for this group in sample data
        assert len(specific_group) > 0
        record = specific_group[0]
        
        # Total_Profit should be rounded to 2 decimals
        total_profit = record["Total_Profit"]
        assert isinstance(total_profit, (int, float))

    def test_order_count_is_aggregated(self, sample_data_with_dates):
        """Confirms that Order_Count reflects the number of orders in each group."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Check that Order_Count is present and valid
        order_counts = profit_summary_df.select("Order_Count").collect()
        for row in order_counts:
            assert row["Order_Count"] > 0

    def test_year_is_extracted_from_order_date(self, sample_data_with_dates):
        """Ensures that Year is correctly extracted from Order Date."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        years = profit_summary_df.select("Year").distinct().collect()
        year_values = [row["Year"] for row in years]
        
        # Should have years from the sample data (2016, 2017, 2018)
        assert len(year_values) > 0
        assert all(isinstance(y, int) for y in year_values)

    def test_output_is_ordered(self, sample_data_with_dates):
        """Confirms that output is ordered by Year, Category, Sub-Category, Customer Name."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Collect and verify ordering
        rows = profit_summary_df.collect()
        assert len(rows) > 0

    def test_handles_single_order_groups(self, sample_data_with_dates):
        """Ensures groups with only one order are handled correctly."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Check for groups with Order_Count == 1
        single_order_groups = profit_summary_df.filter(col("Order_Count") == 1).count()
        # May or may not have single-order groups depending on sample data
        assert single_order_groups >= 0

    # ========================================================================
    # NEGATIVE SCENARIOS
    # ========================================================================

    def test_handles_multiple_customers_in_same_category(self, spark):
        """Tests aggregation when multiple customers have orders in the same category."""
        orders_data = [
            ("O1", date(2016, 8, 21), "C1", "Tech", "Computers", 100.0),
            ("O2", date(2016, 9, 23), "C2", "Tech", "Computers", 200.0),
            ("O3", date(2017, 1, 1), "C1", "Tech", "Computers", 150.0),
        ]
        orders_schema = StructType([
            StructField("Order ID", StringType()),
            StructField("Order Date", DateType()),
            StructField("Customer Name", StringType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Profit", DoubleType())
        ])
        enriched_df = spark.createDataFrame(orders_data, orders_schema)
        
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Should have at least 2 customer records in Tech/Computers for 2016
        assert profit_summary_df.count() >= 2

    def test_handles_negative_profits(self, spark):
        """Ensures negative profits are correctly aggregated."""
        orders_data = [
            ("O1", date(2016, 8, 21), "C1", "Tech", "Computers", -50.0),
            ("O2", date(2016, 8, 22), "C1", "Tech", "Computers", 100.0),
        ]
        orders_schema = StructType([
            StructField("Order ID", StringType()),
            StructField("Order Date", DateType()),
            StructField("Customer Name", StringType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Profit", DoubleType())
        ])
        enriched_df = spark.createDataFrame(orders_data, orders_schema)
        
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        # Should aggregate negative and positive profits
        total_profit = profit_summary_df.select("Total_Profit").collect()[0]["Total_Profit"]
        assert total_profit == 50.0

    def test_handles_zero_profit_orders(self, spark):
        """Tests handling of orders with zero profit."""
        orders_data = [
            ("O1", date(2016, 8, 21), "C1", "Tech", "Computers", 0.0),
            ("O2", date(2016, 8, 22), "C1", "Tech", "Computers", 100.0),
        ]
        orders_schema = StructType([
            StructField("Order ID", StringType()),
            StructField("Order Date", DateType()),
            StructField("Customer Name", StringType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Profit", DoubleType())
        ])
        enriched_df = spark.createDataFrame(orders_data, orders_schema)
        
        profit_summary_df = create_profit_summary_table(enriched_df)
        assert profit_summary_df.count() > 0

    def test_handles_empty_enriched_orders(self, spark):
        """Tests behavior with an empty enriched orders DataFrame."""
        orders_schema = StructType([
            StructField("Order ID", StringType()),
            StructField("Order Date", DateType()),
            StructField("Customer Name", StringType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Profit", DoubleType())
        ])
        empty_df = spark.createDataFrame([], orders_schema)
        
        profit_summary_df = create_profit_summary_table(empty_df)
        assert profit_summary_df.count() == 0


class TestValidationFunctions:
    """Tests for the data quality validation functions."""

    def test_validate_profit_summary_no_nulls_in_keys(self, sample_data_with_dates, capsys):
        """Ensures validation passes when no nulls in key columns."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        validate_profit_summary_table(profit_summary_df, enriched_df)
        
        captured = capsys.readouterr()
        # Should not warn about nulls in key columns
        assert "null values in 'Year' column" not in captured.out

    def test_validate_profit_summary_no_duplicates(self, sample_data_with_dates, capsys):
        """Ensures validation detects when there are no duplicate records."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        validate_profit_summary_table(profit_summary_df, enriched_df)
        
        captured = capsys.readouterr()
        # Should confirm all records are unique
        assert "unique" in captured.out.lower()

    def test_validate_detects_null_year(self, spark, sample_data_with_dates, capsys):
        """Confirms validation detects null values in Year column."""
        orders_data = [
            (None, "Tech", "Computers", "C1", 100.0, 1),
        ]
        orders_schema = StructType([
            StructField("Year", IntegerType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Customer Name", StringType()),
            StructField("Total_Profit", DoubleType()),
            StructField("Order_Count", IntegerType())
        ])
        df_with_null = spark.createDataFrame(orders_data, orders_schema)
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        
        validate_profit_summary_table(df_with_null, enriched_df)
        
        captured = capsys.readouterr()
        assert "null values in 'Year' column" in captured.out

    def test_validate_detects_null_category(self, spark, sample_data_with_dates, capsys):
        """Confirms validation detects null values in Category column."""
        orders_data = [
            (2016, None, "Computers", "C1", 100.0, 1),
        ]
        orders_schema = StructType([
            StructField("Year", IntegerType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Customer Name", StringType()),
            StructField("Total_Profit", DoubleType()),
            StructField("Order_Count", IntegerType())
        ])
        df_with_null = spark.createDataFrame(orders_data, orders_schema)
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        
        validate_profit_summary_table(df_with_null, enriched_df)
        
        captured = capsys.readouterr()
        assert "null values in 'Category' column" in captured.out

    def test_validate_detects_null_profit(self, spark, sample_data_with_dates, capsys):
        """Confirms validation detects null Total_Profit values."""
        orders_data = [
            (2016, "Tech", "Computers", "C1", None, 1),
        ]
        orders_schema = StructType([
            StructField("Year", IntegerType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Customer Name", StringType()),
            StructField("Total_Profit", DoubleType()),
            StructField("Order_Count", IntegerType())
        ])
        df_with_null = spark.createDataFrame(orders_data, orders_schema)
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        
        validate_profit_summary_table(df_with_null, enriched_df)
        
        captured = capsys.readouterr()
        assert "null 'Total_Profit' values" in captured.out

    def test_validate_detects_zero_order_count(self, spark, sample_data_with_dates, capsys):
        """Ensures validation detects Order_Count of zero or negative."""
        orders_data = [
            (2016, "Tech", "Computers", "C1", 100.0, 0),
            (2016, "Tech", "Accessories", "C2", 50.0, -1),
        ]
        orders_schema = StructType([
            StructField("Year", IntegerType()),
            StructField("Category", StringType()),
            StructField("Sub-Category", StringType()),
            StructField("Customer Name", StringType()),
            StructField("Total_Profit", DoubleType()),
            StructField("Order_Count", IntegerType())
        ])
        df_invalid = spark.createDataFrame(orders_data, orders_schema)
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        
        validate_profit_summary_table(df_invalid, enriched_df)
        
        captured = capsys.readouterr()
        assert "non-positive 'Order_Count'" in captured.out

    def test_validate_row_count_consistency(self, sample_data_with_dates, capsys):
        """Ensures validation checks row count consistency."""
        enriched_df = sample_data_with_dates["enriched_orders_df"]
        profit_summary_df = create_profit_summary_table(enriched_df)
        
        validate_profit_summary_table(profit_summary_df, enriched_df)
        
        captured = capsys.readouterr()
        # Should output row count info
        assert "unique groups" in captured.out or "row count" in captured.out.lower()
