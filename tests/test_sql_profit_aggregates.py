import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_profit_aggregates import (
    get_profit_by_year,
    get_profit_by_year_category,
    get_profit_by_customer,
    get_profit_by_customer_year,
    get_all_profit_aggregates,
    validate_aggregate_output,
    validate_all_aggregates
)


@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .appName("test_sql_profit_aggregates") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_enriched_orders_df(spark):
    schema = StructType([
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Category", StringType(), True)
    ])
    
    data = [
        ("ORD-001", "1/1/2014", 100.50, "John Smith", "Technology"),
        ("ORD-002", "2/1/2014", 250.75, "Jane Doe", "Furniture"),
        ("ORD-003", "3/1/2014", 150.25, "John Smith", "Office Supplies"),
        ("ORD-004", "15/6/2015", 300.00, "Jane Doe", "Technology"),
        ("ORD-005", "20/6/2015", 75.50, "Bob Johnson", "Furniture"),
        ("ORD-006", "10/12/2016", 200.00, "John Smith", "Technology"),
        ("ORD-007", "15/12/2016", 125.75, "Alice Brown", "Office Supplies"),
    ]
    
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def empty_orders_df(spark):
    schema = StructType([
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Category", StringType(), True)
    ])
    
    return spark.createDataFrame([], schema=schema)


class TestGetProfitByYear:
    
    def test_profit_by_year_returns_dataframe(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        assert result is not None
        assert result.count() > 0
    
    def test_profit_by_year_has_correct_columns(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        columns = result.columns
        assert "Year" in columns
        assert "Total_Profit" in columns
        assert "Order_Count" in columns
    
    def test_profit_by_year_correct_grouping(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        rows = result.collect()
        
        assert len(rows) == 3
        assert rows[0]["Year"] == 2014
        assert rows[1]["Year"] == 2015
        assert rows[2]["Year"] == 2016
    
    def test_profit_by_year_correct_totals(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        rows = result.collect()
        
        row_2014 = [r for r in rows if r["Year"] == 2014][0]
        assert row_2014["Total_Profit"] == 501.5
        assert row_2014["Order_Count"] == 3
    
    def test_profit_by_year_ordered(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        rows = result.collect()
        years = [r["Year"] for r in rows]
        
        assert years == sorted(years)
    
    def test_profit_by_year_empty_input(self, empty_orders_df):
        result = get_profit_by_year(empty_orders_df)
        assert result.count() == 0


class TestGetProfitByYearCategory:
    
    def test_profit_by_year_category_returns_dataframe(self, sample_enriched_orders_df):
        result = get_profit_by_year_category(sample_enriched_orders_df)
        assert result is not None
        assert result.count() > 0
    
    def test_profit_by_year_category_has_correct_columns(self, sample_enriched_orders_df):
        result = get_profit_by_year_category(sample_enriched_orders_df)
        columns = result.columns
        assert "Year" in columns
        assert "Category" in columns
        assert "Total_Profit" in columns
        assert "Order_Count" in columns
    
    def test_profit_by_year_category_correct_grouping(self, sample_enriched_orders_df):
        result = get_profit_by_year_category(sample_enriched_orders_df)
        rows = result.collect()
        
        assert len(rows) > 0
        
        tech_2014 = [r for r in rows if r["Year"] == 2014 and r["Category"] == "Technology"]
        assert len(tech_2014) == 1
        assert tech_2014[0]["Total_Profit"] == 100.50
        assert tech_2014[0]["Order_Count"] == 1
    
    def test_profit_by_year_category_ordered(self, sample_enriched_orders_df):
        result = get_profit_by_year_category(sample_enriched_orders_df)
        rows = result.collect()
        
        years = [r["Year"] for r in rows]
        assert years == sorted(years)
    
    def test_profit_by_year_category_empty_input(self, empty_orders_df):
        result = get_profit_by_year_category(empty_orders_df)
        assert result.count() == 0


class TestGetProfitByCustomer:
    
    def test_profit_by_customer_returns_dataframe(self, sample_enriched_orders_df):
        result = get_profit_by_customer(sample_enriched_orders_df)
        assert result is not None
        assert result.count() > 0
    
    def test_profit_by_customer_has_correct_columns(self, sample_enriched_orders_df):
        result = get_profit_by_customer(sample_enriched_orders_df)
        columns = result.columns
        assert "Customer Name" in columns
        assert "Total_Profit" in columns
        assert "Order_Count" in columns
    
    def test_profit_by_customer_correct_grouping(self, sample_enriched_orders_df):
        result = get_profit_by_customer(sample_enriched_orders_df)
        rows = result.collect()
        
        assert len(rows) == 4
        
        john_smith = [r for r in rows if r["Customer Name"] == "John Smith"][0]
        assert john_smith["Total_Profit"] == 450.75
        assert john_smith["Order_Count"] == 3
    
    def test_profit_by_customer_ordered_by_profit(self, sample_enriched_orders_df):
        result = get_profit_by_customer(sample_enriched_orders_df)
        rows = result.collect()
        profits = [r["Total_Profit"] for r in rows]
        
        assert profits == sorted(profits, reverse=True)
    
    def test_profit_by_customer_empty_input(self, empty_orders_df):
        result = get_profit_by_customer(empty_orders_df)
        assert result.count() == 0


class TestGetProfitByCustomerYear:
    
    def test_profit_by_customer_year_returns_dataframe(self, sample_enriched_orders_df):
        result = get_profit_by_customer_year(sample_enriched_orders_df)
        assert result is not None
        assert result.count() > 0
    
    def test_profit_by_customer_year_has_correct_columns(self, sample_enriched_orders_df):
        result = get_profit_by_customer_year(sample_enriched_orders_df)
        columns = result.columns
        assert "Customer Name" in columns
        assert "Year" in columns
        assert "Total_Profit" in columns
        assert "Order_Count" in columns
    
    def test_profit_by_customer_year_correct_grouping(self, sample_enriched_orders_df):
        result = get_profit_by_customer_year(sample_enriched_orders_df)
        rows = result.collect()
        
        assert len(rows) > 0
        
        john_2014 = [r for r in rows if r["Customer Name"] == "John Smith" and r["Year"] == 2014]
        assert len(john_2014) == 1
        assert john_2014[0]["Total_Profit"] == 250.75
        assert john_2014[0]["Order_Count"] == 2
    
    def test_profit_by_customer_year_ordered(self, sample_enriched_orders_df):
        result = get_profit_by_customer_year(sample_enriched_orders_df)
        rows = result.collect()
        
        years = [r["Year"] for r in rows]
        assert years == sorted(years)
    
    def test_profit_by_customer_year_empty_input(self, empty_orders_df):
        result = get_profit_by_customer_year(empty_orders_df)
        assert result.count() == 0


class TestGetAllProfitAggregates:
    
    def test_returns_dict_with_all_four_aggregates(self, sample_enriched_orders_df):
        result = get_all_profit_aggregates(sample_enriched_orders_df)
        
        assert isinstance(result, dict)
        assert len(result) == 4
        assert "profit_by_year" in result
        assert "profit_by_year_category" in result
        assert "profit_by_customer" in result
        assert "profit_by_customer_year" in result
    
    def test_all_aggregates_return_dataframes(self, sample_enriched_orders_df):
        result = get_all_profit_aggregates(sample_enriched_orders_df)
        
        for key, df in result.items():
            assert df is not None
            assert hasattr(df, 'count')
    
    def test_all_aggregates_have_required_columns(self, sample_enriched_orders_df):
        result = get_all_profit_aggregates(sample_enriched_orders_df)
        
        for key, df in result.items():
            columns = df.columns
            assert "Total_Profit" in columns
            assert "Order_Count" in columns


class TestValidateAggregateOutput:
    
    def test_validate_valid_dataframe(self, sample_enriched_orders_df):
        df = get_profit_by_year(sample_enriched_orders_df)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is True
    
    def test_validate_empty_dataframe(self, empty_orders_df):
        df = get_profit_by_year(empty_orders_df)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False
    
    def test_validate_with_null_profit(self, spark):
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        data = [
            (2014, 100.0, 2),
            (2015, None, 1),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False
    
    def test_validate_with_null_order_count(self, spark):
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        data = [
            (2014, 100.0, 2),
            (2015, 150.0, None),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False
    
    def test_validate_with_negative_profit(self, spark):
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        data = [
            (2014, 100.0, 2),
            (2015, -50.0, 1),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False
    
    def test_validate_with_zero_order_count(self, spark):
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        data = [
            (2014, 100.0, 2),
            (2015, 150.0, 0),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False
    
    def test_validate_with_negative_order_count(self, spark):
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        data = [
            (2014, 100.0, 2),
            (2015, 150.0, -1),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_aggregate_output(df, "Profit By Year")
        assert result is False


class TestValidateAllAggregates:
    
    def test_validate_all_valid_aggregates(self, sample_enriched_orders_df):
        aggregates = get_all_profit_aggregates(sample_enriched_orders_df)
        result = validate_all_aggregates(aggregates)
        
        assert isinstance(result, dict)
        assert len(result) == 4
        assert all(v is True for v in result.values())
    
    def test_validate_all_returns_dict(self, sample_enriched_orders_df):
        aggregates = get_all_profit_aggregates(sample_enriched_orders_df)
        result = validate_all_aggregates(aggregates)
        
        assert isinstance(result, dict)
        assert "profit_by_year" in result
        assert "profit_by_year_category" in result
        assert "profit_by_customer" in result
        assert "profit_by_customer_year" in result
    
    def test_validate_all_with_mixed_results(self, spark, sample_enriched_orders_df):
        aggregates = get_all_profit_aggregates(sample_enriched_orders_df)
        
        schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Total_Profit", DoubleType(), True),
            StructField("Order_Count", IntegerType(), True)
        ])
        
        invalid_df = spark.createDataFrame([(2014, -100.0, 2)], schema=schema)
        aggregates["profit_by_year"] = invalid_df
        
        result = validate_all_aggregates(aggregates)
        
        assert result["profit_by_year"] is False
        assert result["profit_by_year_category"] is True
        assert result["profit_by_customer"] is True
        assert result["profit_by_customer_year"] is True


class TestDataAccuracy:
    
    def test_profit_calculation_accuracy(self, sample_enriched_orders_df):
        result = get_profit_by_year(sample_enriched_orders_df)
        rows = result.collect()
        
        row_2014 = [r for r in rows if r["Year"] == 2014][0]
        expected_profit = 100.50 + 250.75 + 150.25
        assert round(row_2014["Total_Profit"], 2) == round(expected_profit, 2)
    
    def test_order_count_accuracy(self, sample_enriched_orders_df):
        result = get_profit_by_customer(sample_enriched_orders_df)
        rows = result.collect()
        
        jane_doe = [r for r in rows if r["Customer Name"] == "Jane Doe"][0]
        assert jane_doe["Order_Count"] == 2
    
    def test_distinct_order_counting(self, spark):
        schema = StructType([
            StructField("Order ID", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Profit", DoubleType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("Category", StringType(), True)
        ])
        
        data = [
            ("ORD-001", "1/1/2014", 100.0, "John Smith", "Technology"),
            ("ORD-001", "1/1/2014", 100.0, "John Smith", "Technology"),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = get_profit_by_customer(df)
        rows = result.collect()
        
        assert rows[0]["Order_Count"] == 1
    
    def test_zero_profit_handling(self, spark):
        schema = StructType([
            StructField("Order ID", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Profit", DoubleType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("Category", StringType(), True)
        ])
        
        data = [
            ("ORD-001", "1/1/2014", 0.0, "John Smith", "Technology"),
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = get_profit_by_customer(df)
        rows = result.collect()
        
        assert rows[0]["Total_Profit"] == 0.0
        assert rows[0]["Order_Count"] == 1
