import sys
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, concat, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from pyspark.ml.feature import Bucketizer, StringIndexerModel, VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel


REQUEST_TOPIC = "flight-delay-ml-request"
RESPONSE_TOPIC = "flight-delay-ml-response"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092"
)

CASSANDRA_HOST = os.environ.get(
    "CASSANDRA_HOST",
    "127.0.0.1"
)

MINIO_ENDPOINT = os.environ.get(
    "MINIO_ENDPOINT",
    "http://127.0.0.1:9000"
)

MINIO_ACCESS_KEY = os.environ.get(
    "MINIO_ACCESS_KEY",
    "admin"
)

MINIO_SECRET_KEY = os.environ.get(
    "MINIO_SECRET_KEY",
    "password123"
)


def load_models(model_base_path):
    arrival_bucketizer = Bucketizer.load(
        f"{model_base_path}/arrival_bucketizer_2.0.bin"
    )

    indexers = {}
    for column in ["Carrier", "Origin", "Dest", "Route"]:
        indexers[column] = StringIndexerModel.load(
            f"{model_base_path}/string_indexer_model_{column}.bin"
        )

    vector_assembler = VectorAssembler.load(
        f"{model_base_path}/numeric_vector_assembler.bin"
    )

    rf_model = RandomForestClassificationModel.load(
        f"{model_base_path}/spark_random_forest_classifier.flight_delays.5.0.bin"
    )

    return arrival_bucketizer, indexers, vector_assembler, rf_model


def process_batch(batch_df, batch_id, model_base_path):
    if batch_df.limit(1).count() == 0:
        print("Batch vacío, esperando mensajes...")
        return

    print(f"Procesando batch {batch_id}...")

    arrival_bucketizer, indexers, vector_assembler, rf_model = load_models(model_base_path)

    df = batch_df.withColumn(
        "Route",
        concat(col("Origin"), lit("-"), col("Dest"))
    )

    for column in ["Carrier", "Origin", "Dest", "Route"]:
        df = indexers[column].transform(df)

    df = vector_assembler.transform(df)

    predictions = rf_model.transform(df)

    output = predictions.select(
        col("UUID").alias("uuid"),
        col("Origin").alias("origin"),
        col("Dest").alias("dest"),
        col("Carrier").alias("carrier"),
        col("Route").alias("route"),
        col("FlightDate").alias("flight_date"),
        col("DayOfWeek").cast("int").alias("day_of_week"),
        col("DayOfYear").cast("int").alias("day_of_year"),
        col("DayOfMonth").cast("int").alias("day_of_month"),
        col("DepDelay").cast("double").alias("dep_delay"),
        col("Distance").cast("double").alias("distance"),
        col("Prediction").cast("double").alias("prediction"),
        col("Timestamp").alias("timestamp")
    )

    print("Predicción generada:")
    output.show(truncate=False)

    output.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(
            table="flight_delay_ml_response",
            keyspace="agile_data_science"
        ) \
        .save()

    kafka_output = output.select(
        to_json(
            struct(
                col("uuid").alias("UUID"),
                col("origin").alias("Origin"),
                col("dest").alias("Dest"),
                col("carrier").alias("Carrier"),
                col("route").alias("Route"),
                col("flight_date").alias("FlightDate"),
                col("day_of_week").alias("DayOfWeek"),
                col("day_of_year").alias("DayOfYear"),
                col("day_of_month").alias("DayOfMonth"),
                col("dep_delay").alias("DepDelay"),
                col("distance").alias("Distance"),
                col("prediction").alias("Prediction"),
                col("timestamp").alias("Timestamp")
            )
        ).alias("value")
    )

    kafka_output.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", RESPONSE_TOPIC) \
        .save()

    print(f"Batch {batch_id} procesado correctamente")


def main():
    if len(sys.argv) < 2:
        print("Uso: spark-submit streaming_predictions_kafka_cassandra.py s3a://lakehouse/models")
        sys.exit(1)

    model_base_path = sys.argv[1]

    print("Arrancando Spark Streaming...")
    print("Kafka:", KAFKA_BOOTSTRAP_SERVERS)
    print("Cassandra:", CASSANDRA_HOST)
    print("MinIO:", MINIO_ENDPOINT)
    print("Model path:", model_base_path)

    spark = (
        SparkSession.builder
        .appName("streaming_predictions_kafka_cassandra")

        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config("spark.cassandra.connection.host", CASSANDRA_HOST)

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("UUID", StringType(), True),
        StructField("Carrier", StringType(), True),
        StructField("DayOfMonth", IntegerType(), True),
        StructField("DayOfWeek", IntegerType(), True),
        StructField("DayOfYear", IntegerType(), True),
        StructField("DepDelay", DoubleType(), True),
        StructField("Dest", StringType(), True),
        StructField("Distance", DoubleType(), True),
        StructField("FlightDate", StringType(), True),
        StructField("FlightNum", StringType(), True),
        StructField("Origin", StringType(), True),
        StructField("Timestamp", StringType(), True),
    ])

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", REQUEST_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, model_base_path))
        .option(
            "checkpointLocation",
            "s3a://lakehouse/checkpoints/streaming_predictions_kafka_cassandra"
        )
        .start()
    )

    print("Streaming arrancado. Esperando mensajes en Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()