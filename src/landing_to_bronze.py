# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession


def rename_columns(df):
    """Replace spaces and dashes with underscores in column names"""
    return df.toDF(
        *[col.strip().lower().replace(" ","_").replace("-","_") for col in df.columns]
    )

def load_customer_data(spark):
    #read excel using pandas as pandas dataframe
    pdf = pd.read_excel("/Volumes/ctg_dev/landing/home_volume/PEI/Customer.xlsx",dtype=str)

    #convert pandas dataframe to spark dataframe
    customer_df=spark.createDataFrame(pdf)
    customer_df=rename_columns(customer_df)

    #write to bronze
    customer_df.write.format("delta").mode("append").saveAsTable("ctg_dev.bronze.customers")

def load_orders_data(spark):
    orders_df=spark.read.format("json").option("multiline","true").load("/Volumes/ctg_dev/landing/home_volume/PEI/Orders.json")
    orders_df=rename_columns(orders_df)
    orders_df.write.format("delta").mode("append").saveAsTable("ctg_dev.bronze.orders")

def load_products_data(spark):
    products_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/ctg_dev/landing/home_volume/PEI/Products.csv")

    products_df=rename_columns(products_df)
    products_df.write.format("delta").mode("append").saveAsTable("ctg_dev.bronze.products")

if __name__=='__main__': # pragma: no cover
    spark=SparkSession.builder.appName("landing to bronze").getOrCreate()
    load_customer_data(spark)
    load_orders_data(spark)
    load_products_data(spark)

# COMMAND ----------

