import pytest 
from unittest.mock import patch,MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.landing_to_bronze import *

class TestLandingToBronze:
    def test_rename_columns(self):
        mock_df = MagicMock()
        mock_df.columns = ["Customer Name", "Age-Value"]

        rename_columns(mock_df)

        mock_df.toDF.assert_called_once_with("customer_name", "age_value")

    @patch("src.landing_to_bronze.rename_columns")
    @patch("src.landing_to_bronze.pd.read_excel")
    def test_load_customer_data(self, mock_read, mock_rename):
        # mock pandas df
        mock_pdf = MagicMock()
        mock_read.return_value = mock_pdf

        # mock spark
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_rename.return_value = mock_df

        # mock writer chain
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        load_customer_data(mock_spark)

        # assert
        mock_read.assert_called_once()
        mock_spark.createDataFrame.assert_called_once_with(mock_pdf)
        mock_writer.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("append")
        mock_writer.saveAsTable.assert_called_once_with("ctg_dev.bronze.customers")

    @patch("src.landing_to_bronze.rename_columns")
    def test_load_orders_data(self, mock_rename):

        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader

        mock_df = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_rename.return_value = mock_df

        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        load_orders_data(mock_spark)

        # assert
        mock_reader.format.assert_called_once_with("json")
        mock_reader.option.assert_called_once_with("multiline", "true")
        mock_reader.load.assert_called_once()

        mock_writer.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("append")
        mock_writer.saveAsTable.assert_called_once_with("ctg_dev.bronze.orders")

    @patch("src.landing_to_bronze.rename_columns")
    def test_load_products_data(self, mock_rename):

        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader

        mock_df = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_rename.return_value = mock_df

        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        load_products_data(mock_spark)

        # assert
        mock_reader.format.assert_called_once_with("csv")
        assert mock_reader.option.call_count == 2
        mock_reader.option.assert_any_call("header", "true")
        mock_reader.option.assert_any_call("inferSchema", "true")
        mock_reader.load.assert_called_once()

        mock_writer.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("append")
        mock_writer.saveAsTable.assert_called_once_with("ctg_dev.bronze.products")