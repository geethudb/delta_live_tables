# Databricks notebook source
# MAGIC %md
# MAGIC Pipeline 1 creates 2 tables bronze ,silver and gold tables . Bronze is append only and silver is apply changes target and gold is aggregated output.
# MAGIC
# MAGIC This example uses the [Wine Quality Data Set](https://archive.ics.uci.edu/ml/datasets/wine+quality).
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC * Databricks Runtime 14.2 for Machine Learning or above.
# MAGIC

# COMMAND ----------



#table creation for bronze chain 1

import dlt
from pyspark.sql import functions as f

@dlt.create_table(comment="Raw data")
def dlt_bronze_wine_quality():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("sep",";")
      .option("inferschema","true")
      .option("ignoreCorruptFiles", "true")
      .load("/databricks-datasets/wine-quality/winequality-red.csv"))
  


# COMMAND ----------


from pyspark.sql.functions import col,expr,lit, when
dlt.create_streaming_table(  name = "silver_wine_quality")
dlt.apply_changes(
  target = "silver_wine_quality",
  source = "dlt_bronze_wine_quality",
  keys = ["wine_id","pH"],
  sequence_by = col("fixed_acidity"),
  except_column_list = None,
  stored_as_scd_type = 1
)

# COMMAND ----------

import dlt
@dlt.create_table(comment="gold data")

def dlt_gold_wine_quality():
  return (
    dlt.read("silver_wine_quality")
      .groupBy("wine_id")
      .agg(f.count(f.col("sulphates")).alias("sulphates_count"))
      .select(f.col("wine_id"),f.col("sulphates_count"))
  )

