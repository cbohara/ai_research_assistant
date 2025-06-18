from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, trim, sha2, year, month, dayofmonth, current_date, date_sub

# Parameters
PARTITION_LOOKBACK_DAYS = 7  # how many days back to process

# 1. Setup Spark with Iceberg
spark = (
    SparkSession.builder
    .appName("AcademicPapersPipeline")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")  # or your catalog type
    .getOrCreate()
)

# 2. Load raw data from different sources (example CSV load)
raw_source1 = spark.read.csv("s3://bucket/source1/*.csv", header=True)
raw_source2 = spark.read.csv("s3://bucket/source2/*.csv", header=True)

# 3. Write raw data to separate Iceberg tables with date partitioning (append mode)
def write_with_partitioning(df, path):
    df_partitioned = df.withColumn("year", year(col("publication_date"))) \
                       .withColumn("month", month(col("publication_date"))) \
                       .withColumn("day", dayofmonth(col("publication_date")))
    df_partitioned.write.format("iceberg").mode("append").partitionBy("year", "month", "day").save(path)

write_with_partitioning(raw_source1, "s3://bucket/iceberg/source1_raw")
write_with_partitioning(raw_source2, "s3://bucket/iceberg/source2_raw")

# 4. Normalize relevant columns & create universal hash key helper function
def normalize_and_hash(df):
    normalized_df = df.withColumn("norm_title", lower(trim(col("title")))) \
                      .withColumn("norm_authors", lower(trim(col("authors")))) \
                      .withColumn("norm_year", col("year").cast("string"))
    return normalized_df.withColumn(
        "universal_hash_key",
        sha2(concat_ws("||", col("norm_title"), col("norm_authors"), col("norm_year")), 256)
    )

# 5. Filter raw data to last X days based on publication_date
cutoff_date = date_sub(current_date(), PARTITION_LOOKBACK_DAYS)

raw_source1_filtered = raw_source1.filter(col("publication_date") >= cutoff_date)
raw_source2_filtered = raw_source2.filter(col("publication_date") >= cutoff_date)

# 6. Normalize & hash filtered raw source tables
norm_source1 = normalize_and_hash(raw_source1_filtered)
norm_source2 = normalize_and_hash(raw_source2_filtered)

# 7. Union normalized datasets for deduplication and master merge
combined_df = norm_source1.select("universal_hash_key", "doi", "title", "authors", "year", "publication_date") \
    .unionByName(norm_source2.select("universal_hash_key", "doi", "title", "authors", "year", "publication_date"))

# 8. Add partition columns to combined_df to match master table partitioning
combined_df = combined_df.withColumn("year", year(col("publication_date"))) \
                         .withColumn("month", month(col("publication_date"))) \
                         .withColumn("day", dayofmonth(col("publication_date")))

# 9. Create temp view for merge
combined_df.createOrReplaceTempView("staging_papers")

master_table = "spark_catalog.default.papers_master"

# 10. Merge only partitions within the lookback window
merge_sql = f"""
MERGE INTO {master_table} AS master
USING staging_papers AS staging
ON master.universal_hash_key = staging.universal_hash_key
AND master.year = staging.year
AND master.month = staging.month
AND master.day = staging.day
WHEN MATCHED THEN UPDATE SET
  master.doi = COALESCE(staging.doi, master.doi),
  master.title = COALESCE(staging.title, master.title),
  master.authors = COALESCE(staging.authors, master.authors),
  master.year = COALESCE(staging.year, master.year),
  master.publication_date = COALESCE(staging.publication_date, master.publication_date)
WHEN NOT MATCHED THEN INSERT *
"""

spark.sql(merge_sql)

spark.stop()
