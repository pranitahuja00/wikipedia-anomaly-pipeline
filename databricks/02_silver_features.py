# Databricks notebook source
print("=== Silver Layer: Wiki Edits Feature Engineering ===")

SOURCE_TABLE    = "bronze_wiki_edits"
TARGET_TABLE    = "silver_wiki_edits"
CHECKPOINT_PATH = "/Volumes/workspace/default/checkpoints/silver_wiki_edits"

print(f"Source     : {SOURCE_TABLE}")
print(f"Target     : {TARGET_TABLE}")
print(f"Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

print("Imports loaded")

# COMMAND ----------

NAMESPACE_MAP = {
    0:  "Article",
    1:  "Talk",
    2:  "User",
    3:  "User_Talk",
    4:  "Wikipedia",
    5:  "Wikipedia_Talk",
    6:  "File",
    7:  "File_Talk",
    10: "Template",
    11: "Template_Talk",
    14: "Category",
    15: "Category_Talk",
}

namespace_map_expr = F.create_map(
    *[item for pair in [(F.lit(k), F.lit(v)) for k, v in NAMESPACE_MAP.items()] for item in pair]
)

print(f"Namespace map defined with {len(NAMESPACE_MAP)} entries")

# COMMAND ----------

# Word-boundary regex prevents partial matches (e.g. "rvv" inside a longer token)
REVERT_PATTERN = r"(?i)\b(revert|rv|rvv|undid)\b"

print(f"Revert pattern: {REVERT_PATTERN}")

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("delta")
    .table(SOURCE_TABLE)
)

print(f"Bronze stream reader defined ({len(bronze_stream.schema.fields)} fields)")

# COMMAND ----------

silver_stream = (
    bronze_stream

    # Quality gate
    .filter(F.col("id").isNotNull() & F.col("timestamp").isNotNull())

    # Timestamp: unix epoch seconds (LongType) -> TimestampType
    .withColumn("edit_timestamp",    F.col("timestamp").cast(TimestampType()))
    .withColumn("edit_hour",         F.hour("edit_timestamp"))
    .withColumn("edit_day_of_week",  F.dayofweek("edit_timestamp"))  # 1=Sun, 7=Sat

    # Byte delta features
    .withColumn("bytes_delta",       F.col("length_new").cast("int") - F.col("length_old").cast("int"))
    .withColumn("abs_bytes_delta",   F.abs(F.col("bytes_delta")))

    # Namespace label (unmapped namespaces -> "Other")
    .withColumn("namespace_label",
        F.coalesce(namespace_map_expr[F.col("namespace")], F.lit("Other"))
    )

    # Revert detection — null-safe guard on comment
    .withColumn("is_revert",
        F.when(
            F.col("comment").isNotNull() &
            (F.regexp_extract(F.col("comment"), REVERT_PATTERN, 0) != ""),
            True
        ).otherwise(False)
    )

    # Silver processing timestamp (for latency tracking vs ingested_at)
    .withColumn("silver_processed_at", F.current_timestamp())

    # Drop Kafka operational metadata — not needed downstream
    .drop("offset", "partition", "kafka_timestamp")
)

print("Silver transformations defined")
print("Output columns:")
for f in silver_stream.schema.fields:
    print(f"  {f.name}: {f.dataType}")

# COMMAND ----------

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "false")
    .trigger(availableNow=True)
    .table(TARGET_TABLE)
)

print("Silver write stream started...")
silver_query.awaitTermination()
print("Silver write stream complete.")

# COMMAND ----------

silver_df = spark.read.table(TARGET_TABLE)
total = silver_df.count()

revert_count = silver_df.filter(F.col("is_revert")).count()
bot_count    = silver_df.filter(F.col("bot")).count()

print(f"Total silver rows : {total:,}")
print(f"Revert edits      : {revert_count:,} ({revert_count / total * 100:.2f}%)")
print(f"Bot edits         : {bot_count:,} ({bot_count / total * 100:.2f}%)")
print("Namespace breakdown:")
silver_df.groupBy("namespace_label").count().orderBy("count", ascending=False).show()

display(silver_df.limit(10))
