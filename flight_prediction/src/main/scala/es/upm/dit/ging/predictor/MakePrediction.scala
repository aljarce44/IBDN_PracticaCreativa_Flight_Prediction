package es.upm.dit.ging.predictor

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, Row}

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = SparkSession
      .builder
      .appName("FlightDelayPrediction")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    import spark.implicits._

    val base_path = sys.env.getOrElse(
      "PROJECT_HOME",
      "C:\\Users\\jimen\\OneDrive\\3ºIySD\\IBDN\\practica_creativa"
    )

    val arrivalBucketizer = Bucketizer.load(
      s"$base_path/models/arrival_bucketizer_2.0.bin"
    )

    val columns = Seq("Carrier", "Origin", "Dest", "Route")

    val stringIndexerModels = columns.map { colName =>
      colName -> StringIndexerModel.load(
        s"$base_path/models/string_indexer_model_${colName}.bin"
      )
    }.toMap

    val vectorAssembler = VectorAssembler.load(
      s"$base_path/models/numeric_vector_assembler.bin"
    )

    val rfc = RandomForestClassificationModel.load(
      s"$base_path/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    )

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.StringType)
      .add("FlightDate", DataTypes.StringType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(
      from_json($"value", struct).as("flight")
    )

    val flightDf = flightNestedDf.selectExpr(
      "flight.Origin",
      "flight.DayOfWeek",
      "flight.DayOfYear",
      "flight.DayOfMonth",
      "flight.Dest",
      "flight.DepDelay",
      "flight.Timestamp",
      "flight.FlightDate",
      "flight.Carrier",
      "flight.UUID",
      "flight.Distance",
      "flight.Carrier_index",
      "flight.Origin_index",
      "flight.Dest_index",
      "flight.Route_index"
    )

    val withRoute = flightDf.withColumn(
      "Route",
      concat($"Origin", lit("-"), $"Dest")
    )

    val vectorized = vectorAssembler
      .setHandleInvalid("keep")
      .transform(withRoute)

    val cleaned = vectorized
      .drop("Carrier_index", "Origin_index", "Dest_index", "Route_index")

    val predictions = rfc.transform(cleaned)
      .drop("Features_vec", "indices", "values", "rawPrediction", "probability")

    val query = predictions.writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>

        if (!batchDF.isEmpty) {
          batchDF.persist()

          val kafkaOutputDf = batchDF
            .selectExpr(
              "CAST(UUID AS STRING) as key",
              "to_json(struct(*)) AS value"
            )

          kafkaOutputDf.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "127.0.0.1:9092")
            .option("topic", "flight-delay-ml-response")
            .save()

          val cassandraOutputDf = batchDF.select(
            col("UUID").as("uuid"),
            col("Origin").as("origin"),
            col("Dest").as("dest"),
            col("Carrier").as("carrier"),
            col("Route").as("route"),
            to_date(col("FlightDate")).as("flight_date"),
            col("DayOfWeek").cast("int").as("day_of_week"),
            col("DayOfYear").cast("int").as("day_of_year"),
            col("DayOfMonth").cast("int").as("day_of_month"),
            col("DepDelay").cast("double").as("dep_delay"),
            col("Distance").cast("double").as("distance"),
            col("Prediction").cast("double").as("prediction"),
            to_timestamp(col("Timestamp")).as("timestamp")
          )

          cassandraOutputDf.write
            .format("org.apache.spark.sql.cassandra")
            .options(Map(
              "keyspace" -> "agile_data_science",
              "table" -> "flight_delay_ml_response"
            ))
            .mode("append")
            .save()

          batchDF.unpersist()
        }

        ()
      }
      .option("checkpointLocation", "C:/tmp/checkpoints-response")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}