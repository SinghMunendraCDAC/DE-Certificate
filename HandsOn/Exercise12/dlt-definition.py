# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

read_schema = StructType([
  StructField("event_name", StringType(), True),
  StructField("event_time", TimestampType(), True)])

@dlt.table(
  comment="raw stream table"
)
def test01():
  return (
    # load incrementally
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .schema(read_schema)
      .load("/tmp/source/events/")
  )

@dlt.table(
  comment="cleaned table"
)
def test02():
  return (
    # update incrementally
    dlt.read_stream("test01")
      .dropna()
  )

@dlt.table(
  comment="summary table"
)
def test03():
  # recompute completely
  read_df = dlt.read("test02")
  return (
    read_df
      .groupBy(
        read_df.event_name, 
        window(read_df.event_time, "1 minute"))
      .count()
  )
