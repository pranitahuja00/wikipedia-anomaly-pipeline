# Databricks notebook source
kafka_bootstrap = dbutils.secrets.get(scope="wikipedia-anomaly-pipeline", key="kafka-bootstrap-server")
kafka_api_key = dbutils.secrets.get(scope="wikipedia-anomaly-pipeline", key="kafka-api-key")
kafka_api_secret = dbutils.secrets.get(scope="wikipedia-anomaly-pipeline", key="kafka-api-secret")

print("Secrets loaded successfully")

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_api_secret}';",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}

print("Kafka config ready")

# COMMAND ----------

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, IntegerType
)

wiki_edits_schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("wiki", StringType()),
    StructField("namespace", IntegerType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("comment", StringType()),
    StructField("length_old", IntegerType()),
    StructField("length_new", IntegerType()),
    StructField("revision_old", LongType()),
    StructField("revision_new", LongType()),
    StructField("server_name", StringType()),
])

raw_edits = (
    spark.readStream
    .format("kafka")
    .option("subscribe", "wiki_edits")
    .options(**kafka_options)
    .load()
    .select(
        from_json(col("value").cast("string"), wiki_edits_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset"),
        col("partition"),
    )
    .select(
        "data.*",
        "kafka_timestamp",
        "offset",
        "partition",
        current_timestamp().alias("ingested_at"),
    )
)

print("wiki_edits stream defined")

# COMMAND ----------

wiki_page_schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("wiki", StringType()),
    StructField("namespace", IntegerType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("comment", StringType()),
    StructField("log_type", StringType()),
    StructField("log_action", StringType()),
    StructField("server_name", StringType()),
])

raw_page_events = (
    spark.readStream
    .format("kafka")
    .option("subscribe", "wiki_page_events")
    .options(**kafka_options)
    .load()
    .select(
        from_json(col("value").cast("string"), wiki_page_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset"),
        col("partition"),
    )
    .select(
        "data.*",
        "kafka_timestamp",
        "offset",
        "partition",
        current_timestamp().alias("ingested_at"),
    )
)

print("wiki_page_events stream defined")

# COMMAND ----------

bronze_edits_query = (
    raw_edits.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/bronze_wiki_edits")
    .trigger(availableNow=True)
    .table("bronze_wiki_edits")
)

bronze_page_query = (
    raw_page_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/bronze_wiki_page_events")
    .trigger(availableNow=True)
    .table("bronze_wiki_page_events")
)

bronze_edits_query.awaitTermination()
bronze_page_query.awaitTermination()

# COMMAND ----------

display(spark.read.table("bronze_wiki_edits").limit(10))
