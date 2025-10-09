import boto3, datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

/*
| dataset_name   | source_db    | source_table                  | target_path                                          | target_db    | target_table                       | last_processed | enable |
| -------------- | ------------ | ----------------------------- | ---------------------------------------------------- | ------------ | ---------------------------------- | -------------- | ------ |
| youtube_stats  | youtube_db   | raw_statistics_reference_data | s3://.../youtube/enriched_statistics_reference_data/ | youtube_db   | enriched_statistics_reference_data | 2025-10-07     | true   |
| twitch_streams | streaming_db | raw_twitch_streams            | s3://.../twitch/enriched_streams/                    | streaming_db | enriched_twitch_streams            | null           | true   |



*/

args = getResolvedOptions(sys.argv, ["JOB_NAME", "METADATA_TABLE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(args["METADATA_TABLE"])

# Get active datasets
datasets = table.scan(FilterExpression="enable = :val", ExpressionAttributeValues={":val": True})["Items"]

for d in datasets:
    print(f"Processing dataset: {d['dataset_name']}")
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=d["source_db"],
        table_name=d["source_table"],
        additional_options={"jobBookmarkOption": "job-bookmark-enable"}
    )
    if dyf.count() == 0:
        print(f"No new data for {d['dataset_name']}, skipping...")
        continue

    df = dyf.toDF()
    df.write.mode("append").option("compression", "snappy").parquet(d["target_path"])

    # Update last processed timestamp
    table.update_item(
        Key={"dataset_name": d["dataset_name"]},
        UpdateExpression="SET last_processed = :ts",
        ExpressionAttributeValues={":ts": datetime.datetime.utcnow().isoformat()}
    )

job.commit()
