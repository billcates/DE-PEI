# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,sum as _sum,round,coalesce, lit, col


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

def load_gold(spark):
    """
    Aggregates orders to compute yearly profit metrics
    grouped by product and customer dimensions.
    """
    order_summary_df=read_table(spark,"ctg_dev.silver.enriched_orders")
    
    order_summary_df = order_summary_df.groupBy(
        year('order_date').alias('year'),
        "product_category",
        "product_subcategory",
        "customer_name"
    ).agg(_sum("profit").alias("total_profit"))

    #round profit by two decimal
    order_summary_df = order_summary_df.withColumn("total_profit",round("total_profit",2)).withColumn(
        "product_category",
        coalesce(col("product_category"), lit("NA"))
    ).withColumn(
        "customer_name",
        coalesce("customer_name",lit("NA"))
    )

    write_table(order_summary_df,"ctg_dev.gold.order_summary")
    return order_summary_df

if __name__=='__main__': # pragma: no cover
    spark=SparkSession.builder.appName("silver to gold").getOrCreate()
    load_gold(spark)

# COMMAND ----------

