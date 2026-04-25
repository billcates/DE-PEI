import pytest
from unittest.mock import patch

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

from src.bronze_to_silver import (
    enrich_customer_data,
    enrich_product_data,
    enrich_orders_data
)


class TestBronzeToSilver:

    @patch("src.bronze_to_silver.write_table")
    @patch("src.bronze_to_silver.read_table")
    def test_enrich_customer_data(self, mock_read, mock_write, spark):

        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("phone", StringType(), True),
        ])

        data = [
            ("1", "John@123  Bill", "123-456"),
            ("2", None, "999"),
            ("3", "  Alice!! ", "abc123"),
        ]

        df = spark.createDataFrame(data, schema)
        mock_read.return_value = df

        result = enrich_customer_data(spark)
        result_data = result.collect()

        assert len(result_data) == 2
        assert result_data[0]["phone"] == "123456"
        assert result_data[0]["customer_name"] == "John Bill"
        assert "created_date" in result.columns

        mock_read.assert_called_once()
        mock_write.assert_called_once()

    @patch("src.bronze_to_silver.write_table")
    @patch("src.bronze_to_silver.read_table")
    def test_enrich_product_data(self, mock_read, mock_write, spark):

        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
        ])

        data = [
            ("1", "cat1"),
            ("1", "cat1"),
            ("2", "cat2"),
        ]

        df = spark.createDataFrame(data, schema)
        mock_read.return_value = df

        result = enrich_product_data(spark)

        result_data = result.collect()

        assert len(result_data) == 2 # duplicate removed
        assert set([r["product_id"] for r in result_data]) == {"1", "2"}
        assert "created_date" in result.columns

        mock_write.assert_called_once()

    @patch("src.bronze_to_silver.write_table")
    @patch("src.bronze_to_silver.read_table")
    def test_enrich_orders_data(self, mock_read, mock_write, spark):

        orders_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("profit", DoubleType(), True),
            StructField("order_date", StringType(), True),
        ])

        orders_data = [
            ("1", "p1", 10.567, "21/8/2016"),
        ]

        orders_df = spark.createDataFrame(orders_data, orders_schema)

        customer_df = spark.createDataFrame(
            [("1", "John", "US")],
            ["customer_id", "customer_name", "country"]
        )

        product_df = spark.createDataFrame(
            [("p1", "Tech", "Phones")],
            ["product_id", "category", "sub_category"]
        )

        mock_read.return_value = orders_df

        result = enrich_orders_data(spark, customer_df, product_df)
        result_data = result.collect()

        row = result_data[0]

        # transformation checks
        assert row["profit"] == 10.57
        assert str(row["order_date"]) == "2016-08-21"

        # join checks
        assert row["customer_name"] == "John"
        assert row["country"] == "US"
        assert row["product_category"] == "Tech"
        assert row["product_subcategory"] == "Phones"

        assert "created_date" in result.columns

        mock_write.assert_called_once()