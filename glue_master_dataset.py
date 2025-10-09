import sys, json, boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import SelectFields, ResolveChoice, CastFieldTypes, RenameField
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
# Load config from S3 or local
# ------------------------------------------------------------------------------------------------
config_path = args["CONFIG_PATH"]

def load_config(path):
    if path.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket = path.split("/")[2]
        key = "/".join(path.split("/")[3:])
        content = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        return json.loads(content)
    else:
        with open(path, "r") as f:
            return json.load(f)

config_data = load_config(config_path)

# ------------------------------------------------------------------------------------------------
# Iterate through datasets
# ------------------------------------------------------------------------------------------------
for dataset in config_data["datasets"]:
    print(f"🚀 Processing dataset: {dataset['name']}")

    source_db = dataset["source"]["database"]
    source_table = dataset["source"]["table_name"]
    target_db = dataset["target"]["database"]
    target_table = dataset["target"]["table_name"]
    target_path = dataset["target"]["path"]

    select_fields = dataset["fields"]["select"]
    cast_mappings = dataset["fields"]["cast"]
    rename_mappings = dataset["fields"]["rename"]

    # 1️⃣ Read from Glue Catalog (only new data)
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=source_db,
        table_name=source_table,
        transformation_ctx=f"{dataset['name']}_source",
        additional_options={"jobBookmarkOption": "job-bookmark-enable"}
    )

    if datasource.count() == 0:
        print(f"No new data for {dataset['name']}, skipping...")
        continue

    # 2️⃣ Apply transformations
    selected = SelectFields.apply(frame=datasource, paths=select_fields)
    resolved = ResolveChoice.apply(frame=selected, choice="make_cols_consistent")
    casted = CastFieldTypes.apply(frame=resolved, mappings=cast_mappings)

    renamed = casted
    for old_name, new_name in rename_mappings.items():
        renamed = RenameField.apply(frame=renamed, old_name=old_name, new_name=new_name)

    final_frame = renamed
    final_df = final_frame.toDF()

    # 3️⃣ Write to target S3
    final_df.write.mode("append").option("compression", "snappy").parquet(target_path)

    # 4️⃣ Update Glue Catalog
    out_dyf = DynamicFrame.fromDF(final_df, glueContext, "out_dyf")
    glueContext.write_dynamic_frame.from_catalog(
        frame=out_dyf,
        database=target_db,
        table_name=target_table,
        additional_options={"enableUpdateCatalog": True},
        transformation_ctx=f"{dataset['name']}_output"
    )

job.commit()
