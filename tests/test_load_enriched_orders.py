import pytest
from src.load_enriched_orders import (
    create_enriched_orders_table,
    validate_enriched_orders_table
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

class TestCreateEnrichedOrdersTable:
    """Tests for the create_enriched_orders_table function."""

    def test_joins_and_columns_correct(self, sample_data):
        """Verifies that joins are successful and columns are added."""
        enriched_df = create_enriched_orders_table(sample_data["orders_df"], sample_data["customers_df"], sample_data["products_df"])
        
        assert "Customer Name" in enriched_df.columns
        assert "Country" in enriched_df.columns
        assert "Category" in enriched_df.columns
        assert "Sub-Category" in enriched_df.columns
        assert enriched_df.count() == 3 # Should not include the orphaned order

    def test_profit_is_rounded(self, sample_data):
        """Ensures that the Profit column is rounded to two decimal places."""
        enriched_df = create_enriched_orders_table(sample_data["orders_df"], sample_data["customers_df"], sample_data["products_df"])
        
        profit_for_o1 = enriched_df.filter("`Order ID` == 'O1'").select("Profit").first()[0]
        assert profit_for_o1 == 120.12

        profit_for_o2 = enriched_df.filter("`Order ID` == 'O2'").select("Profit").first()[0]
        assert profit_for_o2 == 5.46

    def test_inner_join_filters_orphaned_orders(self, sample_data):
        """Confirms that orders without a matching customer are excluded."""
        enriched_df = create_enriched_orders_table(sample_data["orders_df"], sample_data["customers_df"], sample_data["products_df"])
        
        assert enriched_df.filter("`Order ID` == 'O4'").count() == 0
        assert enriched_df.count() == 3

class TestValidationFunctions:
    """Tests for the data quality validation functions."""

    def test_validate_enriched_orders_row_count_mismatch(self, spark, sample_data, capsys):
        """Ensures the validation function warns on row count mismatch."""
        enriched_df = create_enriched_orders_table(sample_data["orders_df"], sample_data["customers_df"], sample_data["products_df"])
        validate_enriched_orders_table(enriched_df, sample_data["orders_df"])
        
        captured = capsys.readouterr()
        assert "Warning: Row count mismatch" in captured.out

    def test_validate_enriched_orders_null_values(self, spark, capsys):
        """Confirms the validation function detects nulls in critical columns."""
        # Create a more complete DataFrame with a null in a key column
        schema = StructType([
            StructField("Order ID", StringType()),
            StructField("Order Date", StringType()),
            StructField("Customer Name", StringType()),
            StructField("Product Name", StringType()),
            StructField("Quantity", IntegerType()),
            StructField("Price", DoubleType())
        ])
        data = [("O1", "21/8/2016", None, "Laptop", 1, 1200.0)]
        df_with_null = spark.createDataFrame(data, schema)
        
        # The original df for row count comparison can be the same for this specific test
        validate_enriched_orders_table(df_with_null, df_with_null)
        
        captured = capsys.readouterr()
        assert "Warning: Found 1 null values in 'Customer Name' column." in captured.out
