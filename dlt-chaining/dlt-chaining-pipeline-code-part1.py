# Databricks notebook source
# MAGIC %md
# MAGIC Pipeline 1 creates 3 tables bronze ,silver and gold tables . Bronze is append only and silver is apply changes target and gold is aggregated output.
# MAGIC
# MAGIC This example uses the [Wine Quality Data Set](https://archive.ics.uci.edu/ml/datasets/wine+quality).
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC * Databricks Runtime 14.2 and above

# COMMAND ----------



#table creation for bronze chain 1

import dlt
from pyspark.sql import functions as f

def renameColumns(df):
  renamed_df = df
  for column in df.columns:
      renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
  return renamed_df


  
@dlt.create_table(comment="Raw data")
def dlt_bronze_wine_quality_pipeline1():
  input_df = spark.readStream.format("cloudFiles")\
                    .option("cloudFiles.format", "csv") \
                    .option("header", "true") \
                    .option("sep",";") \
                    .option("inferschema","true")\
                    .option("ignoreCorruptFiles", "true")\
                    .load("/databricks-datasets/wine-quality/")

    # Rename columns so that they are compatible with Feature Store
  renamed_df = renameColumns(input_df)



  return renamed_df
  


# COMMAND ----------


from pyspark.sql.functions import col,expr,lit, when
dlt.create_streaming_table(  name = "dlt_silver_wine_quality_pipeline1")
dlt.apply_changes(
  target = "dlt_silver_wine_quality_pipeline1",
  source = "dlt_bronze_wine_quality_pipeline1",
  keys = ["pH"],
  sequence_by = col("density"),
  except_column_list = None,
  stored_as_scd_type = 1
)

# COMMAND ----------

import dlt
@dlt.create_table(comment="gold data")

def dlt_gold_wine_quality_pipeline1():
  return (
    dlt.read("dlt_silver_wine_quality_pipeline1")
      .groupBy("pH")
      .agg(f.count(f.col("sulphates")).alias("sulphates_count"))
      .select(f.col("pH"),f.col("sulphates_count"))
  )

