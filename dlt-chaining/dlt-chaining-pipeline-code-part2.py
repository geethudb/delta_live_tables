# Databricks notebook source
# MAGIC %md
# MAGIC Pipeline 2 reads from pipeline 1 outputs . This indicates how to chain the DLT pipelines so that output can be used in different pipelines
# MAGIC This example uses the [Wine Quality Data Set](https://archive.ics.uci.edu/ml/datasets/wine+quality).
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC * Databricks Runtime 14.2 and above
# MAGIC

# COMMAND ----------

import dlt
from pyspark.sql import functions as f

#here we read from silver output stream table of dlt pipeline 1 and populate a gold layer table
@dlt.create_table(comment="Raw data")
def dlt_silver2gold_wine_quality_pipeline2():
  
  #catalog used was gj and schema is bldemo, hence while reading the catalog and schema is specified 
 df = spark.readStream.format("delta").table("gj.bldemo.dlt_silver_wine_quality_pipeline1")
 return ( df.groupBy("chlorides")
      .agg(f.count(f.col("pH")).alias("count_ph"))
      .select(f.col("chlorides"),f.col("count_ph")))


# COMMAND ----------


#here we read from silver output stream table of dlt pipeline 1 and populate a gold layer table
@dlt.create_table(comment="Raw data")
def dlt_gold2gold_wine_quality_pipeline2():
  
  #catalog used was gj and schema is bldemo, hence while reading the catalog and schema is specified 
 df = spark.read.table("gj.bldemo.dlt_gold_wine_quality_pipeline1")
 return ( df)

