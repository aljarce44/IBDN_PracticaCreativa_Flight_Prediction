# !/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, to_timestamp, to_date
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Bucketizer, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
  APP_NAME = "train_model_from_iceberg.py"

  spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
  )

  print("Leyendo datos de entrenamiento desde Iceberg...")

  features = spark.table("lakehouse.flight_delay.training_data")

  print("Filas leídas desde Iceberg:", features.count())
  features.printSchema()

  features = (
    features
    .withColumn("CRSArrTime", to_timestamp(col("CRSArrTime")))
    .withColumn("CRSDepTime", to_timestamp(col("CRSDepTime")))
    .withColumn("FlightDate", to_date(col("FlightDate")))
    .withColumn("DayOfMonth", col("DayOfMonth").cast(IntegerType()))
    .withColumn("DayOfWeek", col("DayOfWeek").cast(IntegerType()))
    .withColumn("DayOfYear", col("DayOfYear").cast(IntegerType()))
  )

  null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
  cols_with_nulls = list(filter(lambda x: x[1] > 0, null_counts))
  print("Columnas con nulos:")
  print(cols_with_nulls)

  features_with_route = features.withColumn(
    "Route",
    concat(
      features.Origin,
      lit("-"),
      features.Dest
    )
  )

  print("Muestra de datos con Route:")
  features_with_route.show(6)

  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]

  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )

  model_base_path = "s3a://lakehouse/models"

  arrival_bucketizer_path = f"{model_base_path}/arrival_bucketizer_2.0.bin"
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
  print("Bucketizer guardado en:", arrival_bucketizer_path)

  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index",
      handleInvalid="keep"
    )

    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)

    ml_bucketized_features = ml_bucketized_features.drop(column)

    string_indexer_output_path = f"{model_base_path}/string_indexer_model_{column}.bin"
    string_indexer_model.write().overwrite().save(string_indexer_output_path)
    print("StringIndexer guardado en:", string_indexer_output_path)

  numeric_columns = [
    "DepDelay",
    "Distance",
    "DayOfMonth",
    "DayOfWeek",
    "DayOfYear"
  ]

  index_columns = [
    "Carrier_index",
    "Origin_index",
    "Dest_index",
    "Route_index"
  ]

  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )

  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

  vector_assembler_path = f"{model_base_path}/numeric_vector_assembler.bin"
  vector_assembler.write().overwrite().save(vector_assembler_path)
  print("VectorAssembler guardado en:", vector_assembler_path)

  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  print("Muestra de features finales:")
  final_vectorized_features.show()

  rfc = RandomForestClassifier(
    featuresCol="Features_vec",
    labelCol="ArrDelayBucket",
    predictionCol="Prediction",
    maxBins=4657,
    maxMemoryInMB=1024
  )

  print("Entrenando modelo RandomForest...")
  model = rfc.fit(final_vectorized_features)

  model_output_path = f"{model_base_path}/spark_random_forest_classifier.flight_delays.5.0.bin"
  model.write().overwrite().save(model_output_path)
  print("Modelo RandomForest guardado en:", model_output_path)

  predictions = model.transform(final_vectorized_features)

  evaluator = MulticlassClassificationEvaluator(
    predictionCol="Prediction",
    labelCol="ArrDelayBucket",
    metricName="accuracy"
  )

  accuracy = evaluator.evaluate(predictions)
  print("Accuracy = {}".format(accuracy))

  print("Distribución de predicciones:")
  predictions.groupBy("Prediction").count().show()

  print("Muestra de predicciones:")
  predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

  print("Entrenamiento desde Lakehouse completado correctamente.")
  print("Modelos almacenados en el Lakehouse:", model_base_path)

  spark.stop()


if __name__ == "__main__":
  main()