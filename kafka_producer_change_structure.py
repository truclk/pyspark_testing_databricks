# Databricks notebook source
import json

import msgpack
from loguru import logger
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_timestamp


def get_event_name():
    dbutils.widgets.text("event_name", "-1")  # noqa
    event_name = None
    try:
        event_name = dbutils.widgets.get("event_name")  # noqa
    except Exception as e:
        logger.exception(f"unable read the input params:{str(e)}")
    return event_name


def get_s3_prefix():
    dbutils.widgets.text("s3_prefix", "-1")  # noqa
    s3_prefix = None
    try:
        s3_prefix = dbutils.widgets.get("s3_prefix")  # noqa
    except Exception as e:
        logger.exception(f"unable read the input params:{str(e)}")
    return s3_prefix


def get_kafka_urls():
    dbutils.widgets.text("kafka_urls", "-1")  # noqa
    kafka_urls = None
    try:
        kafka_urls = dbutils.widgets.get("kafka_urls")  # noqa
    except Exception as e:
        logger.exception(f"unable read the input params:{str(e)}")
    return kafka_urls


EVENT_NAME = get_event_name()
S3_PREFIX = get_s3_prefix()
KAFKA_URLS = get_kafka_urls()


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


# COMMAND ----------

source_params = {"source_name": locals().get("source_name", "unknown")}
if not EVENT_NAME:
    EVENT_NAME = locals().get("event_name", "-1")
logger.info(f"event_name:{EVENT_NAME}")
# event_name='k8s_metrics'


def col_names(event_name: str):
    if event_name == "metadata":
        cols = [
            "event_name",
            "extraction_time",
            "now_time",
        ]
    elif event_name == "metrics":
        cols = [
            "event_name",
            "extraction_time",
            "now_time",
        ]

    return cols


# COMMAND ----------


def process_message(x):
    headers = {item[0]: item[1].decode("UTF-8") for item in x.headers}
    data = {**msgpack.unpackb(x.value), **headers}
    # print("-----------------")
    data = flatten_json(data)
    for key in list(data.keys()):
        if not isinstance(key, str):
            continue
        value = data[key]
        del data[key]
        data[key.replace("-", "_").replace("/", "_").replace(".", "_")] = value

    for key in col_names(event_name=EVENT_NAME):
        if key not in data:
            data[key] = None

    # print(data)
    return json.dumps(data)


def writeToTable(df, epochId):
    if not df.rdd.isEmpty():
        df_read = spark.read.json(  # noqa
            df.rdd.map(lambda x: process_message(x)), multiLine=True
        )  # noqa
        df_read = df_read.withColumn("extraction_time", current_timestamp())
        df_read = (
            df_read.withColumn(
                "event_datetime",
                to_timestamp(col("extraction_time"), "yyyy-MM-dd HH:mm:ss"),
            )
            .withColumn(
                "event_date",
                date_format(
                    to_timestamp(col("extraction_time"), "yyyy-MM-dd HH:mm:ss"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "event_date_new",
                date_format(
                    to_timestamp(col("extraction_time"), "yyyy-MM-dd HH:mm:ss"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "event_date_new_2",
                date_format(
                    to_timestamp(col("extraction_time"), "yyyy-MM-dd HH:mm:ss"),
                    "yyyy-MM-dd",
                ),
            )
        )

        df = df_read.filter(f"event_name  = '{EVENT_NAME}'")
        select_cols = col_names(event_name=EVENT_NAME)
        select_cols.append("event_date")
        select_cols.append("event_datetime")
        select_cols.append("event_date_new")
        select_cols.append("event_date_new_2")

        df = df.select(*select_cols)
        df.write.partitionBy("event_name", "event_date").format("parquet").mode(
            "append"
        ).option("path", f"{S3_PREFIX}{EVENT_NAME}").saveAsTable(
            f"truc_test_table_{EVENT_NAME}"
        )


df = (
    spark.readStream.format("kafka")  # noqa
    .option("includeHeaders", True)
    .option(
        "kafka.bootstrap.servers",
        KAFKA_URLS,
    )
    .option("subscribe", "testing.kafka.topic.databricks")
    .option(
        "value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    )
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)
query = (
    df.selectExpr("value", "headers")
    .writeStream.trigger(processingTime="5 seconds")
    .foreachBatch(writeToTable)
    .start()
)

# query.awaitTermination()
