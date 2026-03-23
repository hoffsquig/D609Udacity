import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("accelerometer_landing_to_trusted", {})

database_name = "stedi"
target_table = "accelerometer_trusted"
source_path = "s3://stedi-lakehouse-hoffman/accelerometer_landing/"
target_path = "s3://stedi-lakehouse-hoffman/accelerometer_trusted/"

# read landing accelerometer data directly from s3
accelerometer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_path],
        "recurse": True
    },
    format="json"
).toDF()

# read trusted customers from glue catalog
customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customer_trusted"
).toDF()

# join accelerometer records to only customers who consented
accelerometer_trusted_df = accelerometer_df.join(
    customer_trusted_df,
    accelerometer_df["user"] == customer_trusted_df["email"],
    "inner"
).select(
    accelerometer_df["user"],
    accelerometer_df["timestamp"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)

# purge target path before writing so reruns do not duplicate data
glueContext.purge_s3_path(
    target_path,
    options={"retentionPeriod": 0}
)

# convert back to dynamic frame so glue sink can update catalog
accelerometer_trusted_dyf = DynamicFrame.fromDF(
    accelerometer_trusted_df,
    glueContext,
    "accelerometer_trusted_dyf"
)

# write parquet and register/update glue catalog table
sink = glueContext.getSink(
    path=target_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=database_name,
    catalogTableName=target_table
)
sink.writeFrame(accelerometer_trusted_dyf)

job.commit()