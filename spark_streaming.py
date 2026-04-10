import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import to_timestamp

# -----------------------------
# Windows / Hadoop Fix
# -----------------------------
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("RealTimeHealthMonitoring") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
    ) \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session Started")

# -----------------------------
# Kafka Stream Source
# -----------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_vitals") \
    .option("startingOffsets", "latest") \
    .load()

print("✅ Connected to Kafka")

# -----------------------------
# Convert Kafka Binary → String
# -----------------------------
raw_df = kafka_df.selectExpr("CAST(value AS STRING)")

# -----------------------------
# JSON Schema
# -----------------------------
schema = StructType() \
    .add("patient_id", IntegerType()) \
    .add("heart_rate", IntegerType()) \
    .add("temperature", FloatType()) \
    .add("oxygen_saturation", IntegerType()) \
    .add("respiratory_rate", IntegerType()) \
    .add("timestamp", StringType())

parsed_df = raw_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

print("✅ JSON Parsing Ready")

# -----------------------------
# Health Status Classification
# -----------------------------
classified_df = parsed_df.withColumn(
    "health_status",
    when(
        (col("heart_rate") > 120) |
        (col("temperature") > 39) |
        (col("oxygen_saturation") < 90) |
        (col("respiratory_rate") > 28),
        "CRITICAL"
    ).when(
        (col("heart_rate") > 100) |
        (col("temperature") > 38) |
        (col("oxygen_saturation") < 94) |
        (col("respiratory_rate") > 24),
        "WARNING"
    ).otherwise("NORMAL")
)

# -----------------------------
# Alert Details
# -----------------------------
classified_df = classified_df.withColumn(
    "alert_reason",
    when(col("heart_rate") > 100, "heart_rate")
    .when(col("temperature") > 38, "temperature")
    .when(col("oxygen_saturation") < 94, "oxygen_saturation")
    .when(col("respiratory_rate") > 24, "respiratory_rate")
    .otherwise("none")
)
classified_df = classified_df.withColumn(
    "alert_value",
    when(col("heart_rate") > 100, col("heart_rate").cast("float"))
    .when(col("temperature") > 38, col("temperature"))
    .when(col("oxygen_saturation") < 94, col("oxygen_saturation").cast("float"))
    .when(col("respiratory_rate") > 24, col("respiratory_rate").cast("float"))
    .otherwise(0.0)
)

alerts_df = classified_df.filter(col("health_status") == "CRITICAL")

# -----------------------------
# STREAM 1 : Display All Vitals
# -----------------------------
vitals_query = classified_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "./checkpoints/vitals") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# STREAM 2 : Display Critical Alerts
# -----------------------------
alerts_query = alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "./checkpoints/alerts") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# STREAM 3 : Write to Cassandra
# -----------------------------
def write_to_cassandra(batch_df, batch_id):

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "health_monitoring") \
        .option("table", "vitals") \
        .save()

cassandra_query = classified_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints/cassandra") \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Streaming Started Successfully")

# -----------------------------
# Keep Streaming Running
# -----------------------------
spark.streams.awaitAnyTermination()