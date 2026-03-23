import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("step_trainer_trusted", {})

database_name = "stedi"
target_table = "step_trainer_trusted"
source_path = "s3://stedi-lakehouse-hoffman/step_trainer_landing/"
target_path = "s3://stedi-lakehouse-hoffman/step_trainer_trusted/"

# read landing step trainer data directly from s3
step_trainer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_path],
        "recurse": True
    },
    format="json"
).toDF()

# read curated customers from glue catalog
customers_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customers_curated"
).toDF()

# keep only step trainer records for customers in curated
step_trainer_trusted_df = step_trainer_df.join(
    customers_curated_df,
    step_trainer_df["serialnumber"] == customers_curated_df["serialNumber"],
    "inner"
).select(
    step_trainer_df["sensorReadingTime"],
    step_trainer_df["serialnumber"],
    step_trainer_df["distanceFromObject"]
)

# purge target path before writing so reruns do not duplicate data
glueContext.purge_s3_path(
    target_path,
    options={"retentionPeriod": 0}
)

# convert back to dynamic frame so glue sink can update catalog
step_trainer_trusted_dyf = DynamicFrame.fromDF(
    step_trainer_trusted_df,
    glueContext,
    "step_trainer_trusted_dyf"
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
sink.writeFrame(step_trainer_trusted_dyf)

job.commit()