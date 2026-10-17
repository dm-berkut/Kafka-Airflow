from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import (
    col,
    from_json,
    trim,
    lower,
    initcap,
    regexp_replace,
    when,
    lit,
    current_timestamp,
    expr,
)

# ==========================
# CONFIG
# ==========================

TOPIC_NAME = "task_events_log_berkut"
KAFKA_BOOTSTRAP_SERVERS = "172.17.1.76:9092"

POSTGRES_URL = "jdbc:postgresql://172.17.1.76:5432/kafka_task"
TARGET_TABLE = "kafka_dev.events_log_berkut"
STAGE_TABLE = "kafka_dev.events_log_berkut_stage"

POSTGRES_PROPERTIES = {
    "user": "lab_user",
    "password": "YOUR_PASSWORD",
    "driver": "org.postgresql.Driver",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CHECKPOINT_LOCATION = str(
    PROJECT_ROOT / "checkpoints" / "consumer_events_log_berkut"
)

VALID_ACTIONS = ["login", "logout", "view", "click", "purchase"]

# ==========================
# SPARK SESSION
# ==========================

spark = (
    SparkSession.builder
    .appName("consumer_spark_berkut")
    .getOrCreate()
)

# ==========================
# SCHEMA
# ==========================

event_schema = StructType([
    StructField("id", LongType(), True),
    StructField("user_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", StringType(), True),
    StructField("site_action", StringType(), True),
    StructField("event_date", StringType(), True),
])

# ==========================
# READ FROM KAFKA
# ==========================

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

events_df = (
    raw_kafka_df
    .select(col("value").cast("string").alias("json_value"))
    .select(from_json(col("json_value"), event_schema).alias("data"))
    .select("data.*")
)

# ==========================
# DATA CLEANING
# ==========================

clean_df = (
    events_df
    .withColumn("user_name_trim", trim(col("user_name")))
    .withColumn(
        "user_name_clean",
        initcap(
            lower(
                regexp_replace(col("user_name_trim"), "\\s+", " ")
            )
        )
    )
    .withColumn("email_trim", lower(trim(col("email"))))
    .withColumn(
        "email_clean",
        when(
            col("email_trim").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
            & col("email_trim").rlike(r"^.{1,254}$"),
            col("email_trim"),
        ).otherwise(lit(None))
    )
    .withColumn("age_int", expr("try_cast(trim(age) as int)"))
    .withColumn(
        "age_clean",
        when(
            (col("age_int") >= 1) & (col("age_int") <= 100),
            col("age_int"),
        )
        .otherwise(lit(0))
        .cast("smallint")
    )
    .withColumn(
        "site_action_clean",
        lower(trim(col("site_action")))
    )
    .withColumn(
        "event_date_clean",
        expr("try_to_timestamp(trim(event_date))")
    )
    .filter(col("id").isNotNull())
    .filter(col("user_name_clean").isNotNull())
    .filter(col("user_name_clean") != "")
    .filter(col("user_name_clean").rlike(r"^.{1,99}$"))
    .filter(col("site_action_clean").isin(VALID_ACTIONS))
    .filter(col("event_date_clean").isNotNull())
    .select(
        col("id").cast("long").alias("id"),
        col("user_name_clean").cast("string").alias("user_name"),
        col("email_clean").cast("string").alias("email"),
        col("age_clean").alias("age"),
        col("site_action_clean").cast("string").alias("site_action"),
        col("event_date_clean").alias("event_date"),
        current_timestamp().alias("load_dttm"),
    )
)

# ==========================
# WRITE TO POSTGRES
# ==========================

def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    jvm = spark._sc._gateway.jvm

    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_PROPERTIES["user"])
    props.setProperty("password", POSTGRES_PROPERTIES["password"])

    driver = (
        jvm.Thread
        .currentThread()
        .getContextClassLoader()
        .loadClass("org.postgresql.Driver")
        .newInstance()
    )

    connection = driver.connect(POSTGRES_URL, props)
    statement = connection.createStatement()

    try:
        (
            batch_df
            .dropDuplicates(["id"])
            .write
            .mode("overwrite")
            .jdbc(
                url=POSTGRES_URL,
                table=STAGE_TABLE,
                properties=POSTGRES_PROPERTIES,
            )
        )

        statement.execute(f"""
            INSERT INTO {TARGET_TABLE}
            (
                id,
                user_name,
                email,
                age,
                site_action,
                event_date,
                load_dttm
            )
            SELECT
                id,
                user_name,
                email,
                age,
                site_action,
                event_date,
                load_dttm
            FROM {STAGE_TABLE}
            ON CONFLICT (id) DO NOTHING;
        """)

        statement.execute(f"TRUNCATE TABLE {STAGE_TABLE};")

        print(f"Batch {batch_id}: written")

    finally:
        statement.close()
        connection.close()

# ==========================
# START STREAM
# ==========================

query = (
    clean_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
spark.stop()
