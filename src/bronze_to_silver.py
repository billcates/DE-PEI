# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace,
    lit,
    current_timestamp,
    col,
    round,
    to_date,
    coalesce,
    regexp_replace,
    trim
)


def read_table(spark,table_name):
    """
    reads table from the catalog and returns a dataframe
    """
    return spark.read.table(table_name)

def write_table(df,table_name):
    """
    writes the dataframe as a table in catalog
    """
    df.write.format("delta").mode("append").saveAsTable(table_name)


def enrich_customer_data(spark):
    """
    Drops columns where customer name is NULL
    Cleans phone number filters and keeps only numbers
    Cleans customer name and removes special characters, If empty after cleaning, replace with NA
    Adds current timestamp to created_date
    """
    #load bronze data
    customer_df = read_table(spark,"ctg_dev.bronze.customers")

    customers_cleaned_df = customer_df.dropna(
        subset="customer_name"
    ).withColumn(
        "phone", regexp_replace("phone",r"[^\dx]","")
    ).withColumn(
        "customer_name",coalesce(
        regexp_replace(
            regexp_replace(trim(col("customer_name")), r"[^a-zA-Z\s]+", ""),
        r"\s+"," "
    ),lit("NA")
    )).withColumn(
        "created_date",lit(current_timestamp())
    )

    write_table(customers_cleaned_df,"ctg_dev.silver.customers")
    return customers_cleaned_df

def enrich_product_data(spark):
    """
    Drops duplicates based on product_id
    Adds current timestamp to created_date
    """
    #load bronze data
    product_df = read_table(spark,"ctg_dev.bronze.products")

    products_cleaned_df = product_df.dropDuplicates(["product_id"]).withColumn(
        "created_date",lit(current_timestamp())
    )

    write_table(products_cleaned_df,"ctg_dev.silver.products")
    return products_cleaned_df

def enrich_orders_data(spark,customer_df,product_df):
    """
    Rounds profit to 2 decimals
    Converts order_date to date format
    Joins with customer_df and product_df left join
    """
    #load bronze data
    orders_df = read_table(spark,"ctg_dev.bronze.orders")
    orders_df = orders_df.withColumn(
        "profit",round(col("profit"),2)
    ).withColumn(
        "order_date", to_date("order_date", "d/M/yyyy")
    )

    
    customer_df = customer_df.select("customer_id","customer_name","country")
    order_summary =orders_df.join(customer_df,"customer_id","left").drop("customer_id_j")

    #join with product
    product_df = product_df.select("product_id",col("category").alias("product_category"),col("sub_category").alias("product_subcategory"))
    order_summary = order_summary.join(product_df,"product_id","left").drop("product_id_j")
    order_summary = order_summary.withColumn("created_date",lit(current_timestamp()))
    write_table(order_summary,"ctg_dev.silver.enriched_orders")
    return order_summary
    
if __name__ == "__main__":  # pragma: no cover
  spark=SparkSession.builder.appName("bronze to silver").getOrCreate()
  customer_df=enrich_customer_data(spark)
  product_df=enrich_product_data(spark)
  orders_df=enrich_orders_data(spark,customer_df,product_df)

# COMMAND ----------

