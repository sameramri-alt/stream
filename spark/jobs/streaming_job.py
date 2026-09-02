"""
=================================================================
streaming_job.py — Pipeline streaming Wikimedia vers MinIO
=================================================================

Rôle général du script :
- lire les événements en temps réel depuis Redpanda/Kafka,
- parser le JSON de Wikimedia,
- nettoyer les données,
- écrire les couches Bronze, Silver et Gold dans MinIO.

Le script est la partie transformation du pipeline.
Si vous voulez changer la logique métier des données, c'est ici qu'il faut modifier :
- les filtres (ex : ignorer les bots),
- les colonnes calculées,
- les agrégations Gold,
- les chemins de sortie.

Le dashboard n'est pas l'endroit pour modifier la logique métier ;
il sert surtout à afficher le résultat.
=================================================================
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    date_trunc,
    from_json,
    from_unixtime,
    lit,
    sum,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia-raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/wikimedia_events/")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/wikimedia_cleaned/")
GOLD_TOP_PATH = os.getenv("GOLD_TOP_PATH", "s3a://lakehouse/gold/top_articles/")
GOLD_METRICS_PATH = os.getenv("GOLD_METRICS_PATH", "s3a://lakehouse/gold/metrics_by_wiki/")
# ---- NOUVEAUX CHEMINS GOLD (Améliorations) ----
GOLD_LANG_PATH = os.getenv("GOLD_LANG_PATH", "s3a://lakehouse/gold/edits_by_language/")
GOLD_TIMESERIES_PATH = os.getenv("GOLD_TIMESERIES_PATH", "s3a://lakehouse/gold/edits_timeseries/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://lakehouse/checkpoints/streaming_job/")

length_schema = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True),
])
revision_schema = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True),
])

wikimedia_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", LongType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", length_schema, True),
    StructField("revision", revision_schema, True),
    StructField("server_name", StringType(), True),       # <-- AJOUTÉ : nom du serveur (ex: fr.wikipedia.org)
    StructField("ingestion_timestamp", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("WikimediaLakehouseStreaming")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Redpanda: {REDPANDA_BROKERS} | Topic: {KAFKA_TOPIC}")
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")
print(f"Gold Top: {GOLD_TOP_PATH}")
print(f"Gold Metrics: {GOLD_METRICS_PATH}")
print(f"Gold Lang: {GOLD_LANG_PATH}")
print(f"Gold Timeseries: {GOLD_TIMESERIES_PATH}")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", REDPANDA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_stream = (
    kafka_stream
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("event", from_json(col("raw_json"), wikimedia_schema))
)


def write_batch(batch: DataFrame, batch_id: int) -> None:
    """Ecrit un micro-lot dans les couches Bronze, Silver et Gold."""
    if batch.isEmpty():
        return

    bronze = batch.select(
        "kafka_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    bronze.write.mode("append").parquet(BRONZE_PATH)

    silver = (
        batch.filter(col("event").isNotNull())
        .filter(coalesce(col("event.bot"), lit(False)) == lit(False))
        .select(
            col("event.id").alias("event_id"),
            col("event.type").alias("change_type"),
            col("event.namespace").alias("namespace_id"),
            col("event.title").alias("article_title"),
            col("event.comment").alias("edit_summary"),
            col("event.timestamp").alias("event_timestamp"),
            col("event.user").alias("editor_name"),
            col("event.minor").alias("is_minor_edit"),
            col("event.patrolled").alias("is_patrolled"),
            col("event.length.old").alias("text_size_before"),
            col("event.length.new").alias("text_size_after"),
            (
                col("event.length.new") - col("event.length.old")
            ).alias("size_change"),
            col("event.revision.old").alias("revision_before"),
            col("event.revision.new").alias("revision_after"),
            col("event.ingestion_timestamp").alias("ingestion_timestamp"),
            col("event.server_name").alias("server_name"),  # <-- AJOUTÉ : langue Wikipedia
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
        )
    )
    silver.write.mode("append").parquet(SILVER_PATH)

    # ---- GOLD 1 : Top articles les plus modifiés ----
    gold_top = (
        silver.groupBy("article_title")
        .agg(count("*").alias("total_edits"))
        .orderBy(col("total_edits").desc())
    )
    gold_top.write.mode("append").parquet(GOLD_TOP_PATH)

    # ---- GOLD 2 : Métriques par namespace / type ----
    gold_metrics = (
        silver.groupBy("namespace_id", "change_type")
        .agg(
            count("*").alias("event_count"),
            sum(col("is_minor_edit").cast("long")).alias("minor_edits"),
            sum(col("is_patrolled").cast("long")).alias("patrolled_edits"),
            sum(col("size_change").cast("long")).alias("total_size_delta"),
            sum(col("text_size_after").cast("long")).alias("total_chars_after"),
            sum(when(col("is_minor_edit") == lit(True), 1).otherwise(0)).alias("minor_edit_count"),
            sum(when(col("is_patrolled") == lit(True), 1).otherwise(0)).alias("patrolled_edit_count"),
        )
        .withColumn("namespace_label", col("namespace_id").cast("string"))
    )
    gold_metrics.write.mode("append").parquet(GOLD_METRICS_PATH)

    # ---- GOLD 3 (NOUVEAU) : Nombre d'éditions par langue Wikipedia ----
    # server_name contient par ex. "fr.wikipedia.org", "en.wikipedia.org"
    # On extrait le code langue ("fr", "en") en prenant tout avant le premier "."
    gold_lang = (
        silver.filter(col("server_name").isNotNull())
        .withColumn("wiki_language", col("server_name"))
        .groupBy("wiki_language")
        .agg(count("*").alias("edit_count"))
        .orderBy(col("edit_count").desc())
    )
    gold_lang.write.mode("append").parquet(GOLD_LANG_PATH)

    # ---- GOLD 4 (NOUVEAU) : Time Series — éditions par minute ----
    # On arrondit le timestamp Kafka à la minute pour regrouper les éditions
    gold_ts = (
        silver.withColumn(
            "minute", date_trunc("minute", col("kafka_timestamp"))
        )
        .groupBy("minute")
        .agg(count("*").alias("edits_per_minute"))
        .orderBy("minute")
    )
    gold_ts.write.mode("append").parquet(GOLD_TIMESERIES_PATH)

    print(f"Micro-lot {batch_id}: Bronze, Silver et 4 Gold ecrits")


query = (
    parsed_stream.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming demarre. Ctrl+C pour arreter.")
query.awaitTermination()
