from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import *
import pandas as pd
from sqlalchemy import create_engine

spark = SparkSession.builder \
    .appName("FintechStreamingPipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("transaction_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("merchant_id", IntegerType()),
    StructField("merchant_category", StringType()),
    StructField("payment_method", StringType()),
    StructField("currency", StringType()),
    StructField("amount", DoubleType()),
    StructField("state", StringType()),
    StructField("channel", StringType()),
    StructField("device_type", StringType()),
    StructField("transaction_status", StringType()),
    StructField("is_international", BooleanType()),
    StructField("risk_score", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "payment_events_v2") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

fraud_df = parsed_df.withColumn(
    "fraud_flag",
    when(col("risk_score") > 0.8, "HIGH_RISK")
    .when(col("amount") > 4000, "HIGH_AMOUNT")
    .when(col("is_international") == True, "INTERNATIONAL")
    .otherwise("NORMAL")
)

engine = create_engine(
    "postgresql://de_user:de_pass@localhost:5433/streaming_dw"
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    pdf = batch_df.toPandas()

    # 1. write raw streaming events
    pdf.to_sql(
        "payment_events_stream",
        engine,
        if_exists="append",
        index=False
    )

    # 2. build batch-level metrics
    total_transactions = len(pdf)
    fraud_transactions = len(pdf[pdf["fraud_flag"] != "NORMAL"])
    fraud_rate = fraud_transactions / total_transactions if total_transactions > 0 else 0
    avg_amount = float(pdf["amount"].mean()) if total_transactions > 0 else 0

    metrics_df = pd.DataFrame([{
        "batch_id": int(batch_id),
        "total_transactions": int(total_transactions),
        "fraud_transactions": int(fraud_transactions),
        "fraud_rate": float(fraud_rate),
        "avg_amount": float(avg_amount)
    }])

    metrics_df.to_sql(
        "payment_metrics_stream",
        engine,
        if_exists="append",
        index=False
    )

    print(
        f"batch {batch_id} written | "
        f"rows={total_transactions} | "
        f"fraud={fraud_transactions} | "
        f"fraud_rate={fraud_rate:.4f} | "
        f"avg_amount={avg_amount:.2f}"
    )


query = fraud_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/payment_events_checkpoint") \
    .start()

query.awaitTermination()
