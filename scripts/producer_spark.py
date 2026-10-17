from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    struct,
    to_json,
    max as spark_max,
    current_timestamp,
)

# ==========================
# CONFIG
# ==========================

TOPIC_NAME = "task_events_log_berkut"

KAFKA_BOOTSTRAP_SERVERS = "172.17.1.76:9092"

POSTGRES_URL = "jdbc:postgresql://172.17.1.76:5432/kafka_task"

POSTGRES_PROPERTIES = {
    "user": "lab_user",
    "password": "YOUR_PASSWORD",
    "driver": "org.postgresql.Driver",
}

SOURCE_TABLE = "kafka_prod.events_source"
OFFSET_TABLE = "kafka_dev.producer_offset_berkut"

# ==========================
# SPARK
# ==========================

spark = (
    SparkSession.builder
    .appName("producer_spark_berkut")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

# ==========================
# READ LAST OFFSET
# ==========================

offset_df = (
    spark.read.jdbc(
        url=POSTGRES_URL,
        table=OFFSET_TABLE,
        properties=POSTGRES_PROPERTIES,
    )
)

offset_row = (
    offset_df
    .filter(col("topic_name") == TOPIC_NAME)
    .select("last_id")
    .collect()
)

last_id = offset_row[0]["last_id"] if offset_row else 0

print(f"Last sent id: {last_id}")

# ==========================
# READ NEW DATA
# ==========================

source_df = (
    spark.read.jdbc(
        url=POSTGRES_URL,
        table=SOURCE_TABLE,
        properties=POSTGRES_PROPERTIES,
    )
    .filter(col("id") > last_id)
)

new_rows_count = source_df.count()

if new_rows_count == 0:
    print("No new rows to send.")
    spark.stop()
    exit(0)

print(f"New rows to send: {new_rows_count}")

# ==========================
# SEND TO KAFKA
# ==========================

kafka_df = (
    source_df.select(
        col("id").cast("string").alias("key"),
        to_json(
            struct(
                "id",
                "user_name",
                "email",
                "age",
                "site_action",
                "event_date",
            )
        ).alias("value"),
    )
)

(
    kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", TOPIC_NAME)
    .save()
)

# ==========================
# UPDATE OFFSET
# ==========================

max_id = (
    source_df
    .select(spark_max("id").alias("max_id"))
    .collect()[0]["max_id"]
)

new_offset_df = (
    spark.createDataFrame(
        [(TOPIC_NAME, max_id)],
        ["topic_name", "last_id"],
    )
    .withColumn("updated_at", current_timestamp())
)

updated_offsets_df = (
    offset_df
    .filter(col("topic_name") != TOPIC_NAME)
    .unionByName(new_offset_df)
)

(
    updated_offsets_df.write
    .jdbc(
        url=POSTGRES_URL,
        table=OFFSET_TABLE,
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
)

print(f"Data successfully written to Kafka topic '{TOPIC_NAME}'")
print(f"Updated last_id = {max_id}")

spark.stop()