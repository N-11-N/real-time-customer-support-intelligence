from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from .governance import mask_pii

SUPPORT_EVENT_SCHEMA = StructType([
    StructField("schema_version", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("ticket_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("subject", StringType(), False),
    StructField("message", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("priority", StringType(), False),
])


def create_spark() -> SparkSession:
    builder = (SparkSession.builder.appName("SupportIntelligence")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_bronze(spark, records, path: str):
    # An explicit, non-nullable schema prevents silent column/type drift.
    ordered = [{field.name: record[field.name] for field in SUPPORT_EVENT_SCHEMA} for record in records]
    df = spark.createDataFrame(ordered, SUPPORT_EVENT_SCHEMA).withColumn("ingested_at", F.current_timestamp())
    df.write.format("delta").mode("append").option("mergeSchema", "false").save(path)
    return df


def upsert_silver(spark, bronze_df, path: str):
    mask_udf = F.udf(lambda value: mask_pii(value or "")[0], "string")
    silver = (bronze_df.dropDuplicates(["event_id"])
        .withColumn("message", mask_udf("message"))
        .withColumn("subject", mask_udf("subject"))
        .withColumn("processed_at", F.current_timestamp()))
    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        (target.alias("t").merge(silver.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        silver.write.format("delta").mode("overwrite").save(path)
    return spark.read.format("delta").load(path)


def build_gold(silver_df, path: str):
    gold = (silver_df.groupBy("priority", "channel")
        .agg(F.countDistinct("ticket_id").alias("ticket_count"),
             F.max("processed_at").alias("last_updated")))
    gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return gold
