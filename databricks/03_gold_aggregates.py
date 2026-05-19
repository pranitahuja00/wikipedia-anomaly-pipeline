# Databricks notebook source
print("=== Gold Layer: Edit Volume Aggregates & Anomaly Detection ===")

SOURCE_TABLE        = "silver_wiki_edits"
VOLUME_TABLE        = "gold_edit_volume"
USER_ACTIVITY_TABLE = "gold_user_activity"

WINDOW_1MIN  = "1 minute"
WINDOW_5MIN  = "5 minutes"

ANOMALY_LOOKBACK_SECONDS  = 3600   # 1-hour rolling window for Z-score baseline
ANOMALY_ZSCORE_THRESHOLD  = 2.0    # flag if edit_count > mean + N * stddev

# Only reprocess the last N days of silver data each run instead of full history
LOOKBACK_DAYS = 2

print(f"Source         : {SOURCE_TABLE}")
print(f"Volume table   : {VOLUME_TABLE}")
print(f"User table     : {USER_ACTIVITY_TABLE}")
print(f"Anomaly window : {ANOMALY_LOOKBACK_SECONDS}s, threshold: {ANOMALY_ZSCORE_THRESHOLD} sigma")
print(f"Lookback days  : {LOOKBACK_DAYS}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

print("Imports loaded")

# COMMAND ----------

from datetime import datetime, timedelta

lookback_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

silver_df = (
    spark.read.table(SOURCE_TABLE)
    .filter(F.col("edit_timestamp") >= F.lit(lookback_date))
)

total_rows = silver_df.count()
time_range = silver_df.agg(
    F.min("edit_timestamp").alias("earliest"),
    F.max("edit_timestamp").alias("latest")
).first()

print(f"Loaded {total_rows:,} rows from {SOURCE_TABLE} (since {lookback_date})")
print(f"Time range: {time_range['earliest']} -> {time_range['latest']}")

# COMMAND ----------

def build_window_agg(df, window_duration, window_label):
    """Tumbling window aggregates over edit_timestamp."""
    return (
        df.groupBy(
            F.window("edit_timestamp", window_duration).alias("time_window")
        )
        .agg(
            F.count("*").alias("total_edits"),
            F.sum(F.col("bot").cast("int")).alias("bot_edits"),
            F.sum((~F.col("bot")).cast("int")).alias("human_edits"),
            F.countDistinct("user").alias("unique_users"),
            F.sum("abs_bytes_delta").alias("total_bytes_changed"),
            F.avg("abs_bytes_delta").alias("avg_bytes_per_edit"),
            F.sum(F.col("is_revert").cast("int")).alias("revert_count"),
        )
        .withColumn("window_start",    F.col("time_window.start"))
        .withColumn("window_end",      F.col("time_window.end"))
        .withColumn("window_duration", F.lit(window_label))
        .withColumn("bot_ratio",
            F.round(F.col("bot_edits") / F.col("total_edits"), 4)
        )
        .withColumn("date", F.to_date("window_start"))
        .drop("time_window")
        .orderBy("window_start")
    )


agg_1min = build_window_agg(silver_df, WINDOW_1MIN, "1min")
agg_5min = build_window_agg(silver_df, WINDOW_5MIN, "5min")

print(f"1-min windows : {agg_1min.count():,}")
print(f"5-min windows : {agg_5min.count():,}")

# COMMAND ----------

def add_anomaly_flags(agg_df):
    """
    Adds rolling Z-score anomaly flag to windowed aggregates.
    Uses a trailing look-back window ordered by window_start (unix seconds).
    rangeBetween on unix_timestamp gives exact time-based range (not row-count based).
    """
    rolling_window = (
        Window
        .orderBy(F.unix_timestamp("window_start"))
        .rangeBetween(-ANOMALY_LOOKBACK_SECONDS, 0)
    )

    return (
        agg_df
        .withColumn("rolling_mean_edits",
            F.avg("total_edits").over(rolling_window)
        )
        # stddev is null for single-row windows; coalesce to 0 so first point is never falsely flagged
        .withColumn("rolling_stddev_edits",
            F.coalesce(F.stddev("total_edits").over(rolling_window), F.lit(0.0))
        )
        .withColumn("anomaly_threshold",
            F.col("rolling_mean_edits") + (ANOMALY_ZSCORE_THRESHOLD * F.col("rolling_stddev_edits"))
        )
        .withColumn("is_anomaly",
            F.col("total_edits") > F.col("anomaly_threshold")
        )
        .withColumn("z_score",
            F.when(
                F.col("rolling_stddev_edits") > 0,
                (F.col("total_edits") - F.col("rolling_mean_edits")) / F.col("rolling_stddev_edits")
            ).otherwise(F.lit(0.0))
        )
    )


gold_volume = add_anomaly_flags(agg_1min).unionByName(add_anomaly_flags(agg_5min))

total_windows = gold_volume.count()
anomaly_count = gold_volume.filter(F.col("is_anomaly")).count()
print(f"Total windows      : {total_windows:,}")
print(f"Anomalous windows  : {anomaly_count:,} ({anomaly_count / total_windows * 100:.1f}%)")

# COMMAND ----------

def build_user_agg(df, window_duration, window_label):
    """Per-user per-window aggregates for identifying high-frequency editors."""
    return (
        df.groupBy(
            F.window("edit_timestamp", window_duration).alias("time_window"),
            F.col("user"),
            F.col("bot"),
        )
        .agg(
            F.count("*").alias("edit_count"),
            F.sum("abs_bytes_delta").alias("bytes_changed"),
            F.sum(F.col("is_revert").cast("int")).alias("revert_count"),
            F.countDistinct("title").alias("unique_pages_edited"),
        )
        .withColumn("window_start",    F.col("time_window.start"))
        .withColumn("window_end",      F.col("time_window.end"))
        .withColumn("window_duration", F.lit(window_label))
        .withColumn("date", F.to_date("window_start"))
        .drop("time_window")
        .orderBy("window_start", F.col("edit_count").desc())
    )


gold_user_activity = (
    build_user_agg(silver_df, WINDOW_1MIN, "1min")
    .unionByName(build_user_agg(silver_df, WINDOW_5MIN, "5min"))
)

print(f"Total user-activity rows : {gold_user_activity.count():,}")
print("Top editors by edit count (5-min windows):")
build_user_agg(silver_df, WINDOW_5MIN, "5min").select(
    "window_start", "user", "bot", "edit_count", "unique_pages_edited"
).show(10)

# COMMAND ----------

def write_partitioned(df, table_name, lookback_date):
    """
    First run: full overwrite to create the table with correct partitioning.
    Subsequent runs: replace only the reprocessed date partitions.
    """
    writer = df.write.format("delta").partitionBy("date")

    if spark.catalog.tableExists(table_name):
        (
            writer
            .mode("overwrite")
            .option("replaceWhere", f"date >= '{lookback_date}'")
            .saveAsTable(table_name)
        )
    else:
        (
            writer
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    print(f"Written {table_name} successfully.")


write_partitioned(gold_volume, VOLUME_TABLE, lookback_date)

# COMMAND ----------

write_partitioned(gold_user_activity, USER_ACTIVITY_TABLE, lookback_date)

# COMMAND ----------

vol_df  = spark.read.table(VOLUME_TABLE)
user_df = spark.read.table(USER_ACTIVITY_TABLE)

print("=== gold_edit_volume ===")
print(f"  Total rows     : {vol_df.count():,}")
print(f"  Anomalous rows : {vol_df.filter('is_anomaly').count():,}")
print("  Window durations:")
vol_df.groupBy("window_duration").count().show()

print("=== gold_user_activity ===")
print(f"  Total rows     : {user_df.count():,}")
print(f"  Distinct users : {user_df.select('user').distinct().count():,}")

display(vol_df.orderBy("window_start", ascending=False).limit(20))
