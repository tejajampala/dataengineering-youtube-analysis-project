import sys, json
from awsglue.transforms import SelectFields, ResolveChoice, CastFieldTypes, RenameField
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

# ------------------------------------------------------------------------------------------------
# Initialize Glue job
# ------------------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------------------
# Load JSON config from S3 or local
# ------------------------------------------------------------------------------------------------
config_path = args["CONFIG_PATH"]
if config_path.startswith("s3://"):
    import boto3
    s3 = boto3.client("s3")
    bucket = config_path.split("/")[2]
    key = "/".join(config_path.split("/")[3:])
    config_data = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
else:
    with open(config_path) as f:
        config_data = json.load(f)

# ------------------------------------------------------------------------------------------------
# 1️⃣ Read only new data from Glue Catalog
# ------------------------------------------------------------------------------------------------
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube_db",
    table_name="raw_statistics_reference_data",
    transformation_ctx="datasource0",
    additional_options={"jobBookmarkOption": "job-bookmark-enable"}  # ✅ Incremental load
)

# ------------------------------------------------------------------------------------------------
# 2️⃣ Select only fields from config
# ------------------------------------------------------------------------------------------------
selected = SelectFields.apply(frame=datasource0, paths=config_data["select_fields"])

# ------------------------------------------------------------------------------------------------
# 3️⃣ Resolve and cast data types
# ------------------------------------------------------------------------------------------------
resolved = ResolveChoice.apply(frame=selected, choice="make_cols_consistent")

casted = CastFieldTypes.apply(frame=resolved, mappings=config_data["cast_mappings"])

# ------------------------------------------------------------------------------------------------
# 4️⃣ Rename columns dynamically from config
# ------------------------------------------------------------------------------------------------
renamed_frame = casted
for old_name, new_name in config_data["rename_mappings"].items():
    renamed_frame = RenameField.apply(
        frame=renamed_frame,
        old_name=old_name,
        new_name=new_name
    )

final_frame = renamed_frame

# ------------------------------------------------------------------------------------------------
# 5️⃣ Write enriched data to S3 (append or overwrite)
# ------------------------------------------------------------------------------------------------
df = final_frame.toDF()

(
    df.write
      .mode("append")   # or "overwrite" if you want full refresh
      .option("compression", "snappy")
      .parquet(config_data["target_path"])
)

# ------------------------------------------------------------------------------------------------
# 6️⃣ Update Glue Catalog table automatically
# ------------------------------------------------------------------------------------------------
out_dyf = DynamicFrame.fromDF(df, glueContext, "out_dyf")

glueContext.write_dynamic_frame.from_catalog(
    frame=out_dyf,
    database=config_data["database"],
    table_name=config_data["target_table"],
    additional_options={"enableUpdateCatalog": True},
    transformation_ctx="s3output"
)

# ------------------------------------------------------------------------------------------------
# 7️⃣ Commit the job (important for bookmarks)
# ------------------------------------------------------------------------------------------------
job.commit()
