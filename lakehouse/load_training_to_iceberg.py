from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROJECT_HOME = r"C:\Users\jimen\OneDrive\3ºIySD\IBDN\practica_creativa"

spark = (
    SparkSession.builder
    .appName("LoadTrainingDataToIceberg")
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

training_path = f"{PROJECT_HOME}\\data\\simple_flight_delay_features.jsonl.bz2"

df = spark.read.json(training_path)

print("Filas leídas del dataset de entrenamiento:", df.count())
print("Columnas:")
print(df.columns)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.flight_delay")

(
    df.writeTo("lakehouse.flight_delay.training_data")
    .using("iceberg")
    .createOrReplace()
)

print("Tabla Iceberg creada: lakehouse.flight_delay.training_data")

check_df = spark.table("lakehouse.flight_delay.training_data")

print("Filas leídas desde Iceberg:", check_df.count())
check_df.printSchema()
check_df.show(5, truncate=False)

spark.stop()