from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

SILVER_PATH = spark.conf.get("spark.silver.path")
GOLD_PATH = spark.conf.get("spark.gold.path")

silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

gold_stream = (
    silver_stream
    .withWatermark("timestamp", "1 minute")
    .groupBy(
        window("timestamp", "1 minute"),
        "state"
    )
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_items")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "state",
        "total_sales",
        "total_items"
    )
)

(
    gold_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", GOLD_PATH + "/_checkpoint")
    .start(GOLD_PATH)
    .awaitTermination()
)
