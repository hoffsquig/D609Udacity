import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("customer_trusted_to_curated", {})

database_name = "stedi"
target_table = "customers_curated"
target_path = "s3://stedi-lakehouse-hoffman/customers_curated/"

#read trusted customers from glue catalog
customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customer_trusted"
).toDF()

#read trusted accelerometer data from glue catalog
accelerometer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="accelerometer_trusted"
).toDF()

#keep only unique customers who have accelerometer data
customers_curated_df = customer_trusted_df.join(
    accelerometer_trusted_df,
    customer_trusted_df["email"] == accelerometer_trusted_df["user"],
    "inner"
).select(
    customer_trusted_df["customerName"],
    customer_trusted_df["email"],
    customer_trusted_df["phone"],
    customer_trusted_df["birthDay"],
    customer_trusted_df["serialNumber"],
    customer_trusted_df["registrationDate"],
    customer_trusted_df["lastUpdateDate"],
    customer_trusted_df["shareWithResearchAsOfDate"],
    customer_trusted_df["shareWithPublicAsOfDate"],
    customer_trusted_df["shareWithFriendsAsOfDate"]
).dropDuplicates()

#purge target path before writing so reruns do not duplicate data
glueContext.purge_s3_path(
    target_path,
    options={"retentionPeriod": 0}
)

#convert back to dynamic frame so glue sink can update catalog
customers_curated_dyf = DynamicFrame.fromDF(
    customers_curated_df,
    glueContext,
    "customers_curated_dyf"
)

#write parquet and register/update glue catalog table
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
sink.writeFrame(customers_curated_dyf)

job.commit()