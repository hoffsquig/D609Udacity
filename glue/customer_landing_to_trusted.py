import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("customer_landing_to_trusted", {})

database_name = "stedi"
target_table = "customer_trusted"
source_path = "s3://stedi-lakehouse-hoffman/customer_landing/"
target_path = "s3://stedi-lakehouse-hoffman/customer_trusted/"

# read landing customer data directly from s3
customer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_path],
        "recurse": True
    },
    format="json"
).toDF()

# filter customers who consented to research
customer_trusted_df = customer_df.filter(
    customer_df["shareWithResearchAsOfDate"].isNotNull()
)

# purge target path before writing so reruns do not duplicate data
glueContext.purge_s3_path(
    target_path,
    options={"retentionPeriod": 0}
)

# convert back to dynamic frame so glue sink can update catalog
customer_trusted_dyf = DynamicFrame.fromDF(
    customer_trusted_df,
    glueContext,
    "customer_trusted_dyf"
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
sink.writeFrame(customer_trusted_dyf)

job.commit()