import pytest
from unittest.mock import patch
from datetime import date

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from src.silver_to_gold import load_gold


class TestGoldLayer:

    @patch("src.silver_to_gold.write_table")
    @patch("src.silver_to_gold.read_table")
    def test_load_gold(self, mock_read, mock_write, spark):
        schema = StructType([
            StructField("order_date", DateType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_subcategory", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("profit", DoubleType(), True),
        ])

        data = [
            (date(2016, 8, 21), "Tech", "Phones", "John", 10.567),
            (date(2016, 8, 22), "Tech", "Phones", "John", 5.333),
            (date(2016, 8, 23), None, "Phones", None, 4.0),
        ]

        df = spark.createDataFrame(data, schema)
        mock_read.return_value = df

        result = load_gold(spark)
        result_data = result.collect()

        # convert to lookup map
        result_map = {
            (r["year"], r["product_category"], r["customer_name"]): r
            for r in result_data
        }

        # aggregation + rounding
        assert result_map[(2016, "Tech", "John")]["total_profit"] == 15.9

        # null handling
        assert result_map[(2016, "NA", "NA")]["total_profit"] == 4.0
        assert "year" in result.columns
        assert "total_profit" in result.columns

        mock_write.assert_called_once_with(result, "ctg_dev.gold.order_summary")