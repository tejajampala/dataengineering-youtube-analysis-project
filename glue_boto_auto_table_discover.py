import boto3, sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE", "TARGET_PREFIX"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_db = args["SOURCE_DATABASE"]
target_prefix = args["TARGET_PREFIX"]   # e.g. s3://my-bucket/processed/
glue_client = boto3.client("glue")

# Get all tables from the database
tables = glue_client.get_tables(DatabaseName=source_db)["TableList"]

for table in tables:
    table_name = table["Name"]
    print(f"Processing table: {table_name}")

    # 1️⃣ Read from Glue Catalog
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=source_db,
        table_name=table_name,
        transformation_ctx=f"{table_name}_source",
        additional_options={"jobBookmarkOption": "job-bookmark-enable"}
    )

    if dyf.count() == 0:
        print(f"No new data for {table_name}, skipping...")
        continue

    # 2️⃣ Example transform: remove nulls
    cleaned_df = dyf.toDF().na.drop("all")

    # 3️⃣ Write to S3 target
    target_path = f"{target_prefix}{source_db}/{table_name}/"
    cleaned_df.write.mode("append").parquet(target_path)

    # 4️⃣ Register target table in Glue Catalog (optional)
    out_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, f"{table_name}_out")
    glueContext.write_dynamic_frame.from_catalog(
        frame=out_dyf,
        database=source_db,
        table_name=f"enriched_{table_name}",
        additional_options={"enableUpdateCatalog": True},
        transformation_ctx=f"{table_name}_output"
    )

job.commit()
