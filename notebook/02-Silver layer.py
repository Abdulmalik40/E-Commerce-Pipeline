from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

BRONZE_PATH = spark.conf.get("spark.bronze.path")
SILVER_PATH = spark.conf.get("spark.silver.path")

spark.readStream \
    .format("delta") \
    .load(BRONZE_PATH) \
    .createOrReplaceTempView("bronze")

silver = spark.sql("""
    SELECT DISTINCT
        order_id,
        to_timestamp(timestamp) AS timestamp,
        customer_id,
        product_id,
        category,
        COALESCE(price, 0.0) AS price,
        COALESCE(quantity, 1) AS quantity,
        COALESCE(price, 0.0) * COALESCE(quantity, 1) AS total_amount,
        city,
        state,
        country,
        latitude,
        longitude,
        delivery_status
    FROM bronze
    WHERE country = 'USA'
      AND state IS NOT NULL
""")

(
    silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_PATH + "/_checkpoint")
    .start(SILVER_PATH)
    .awaitTermination()
)
