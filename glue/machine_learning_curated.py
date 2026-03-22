import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

#job args
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

#contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#constants
database_name = "stedi"
bucket_name = "stedi-lakehouse-hoffman"
target_path = f"s3://{bucket_name}/machine_learning_curated/"
target_table = "machine_learning_curated"

#read trusted sources from data catalog
step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_dyf"
)

accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_dyf"
)

#temp views
step_trainer_trusted_df = step_trainer_trusted_dyf.toDF()
accelerometer_trusted_df = accelerometer_trusted_dyf.toDF()

step_trainer_trusted_df.createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")

#join by step trainer sensor reading time and accelerometer timestamp
machine_learning_curated_df = spark.sql("""
    select
        st.sensorReadingTime,
        st.serialNumber,
        st.distanceFromObject,
        a.user,
        a.timestamp,
        a.x,
        a.y,
        a.z
    from step_trainer_trusted st
    join accelerometer_trusted a
      on st.sensorReadingTime = a.timestamp
""")

#convert back to dynamic frame
machine_learning_curated_dyf = DynamicFrame.fromDF(
    machine_learning_curated_df,
    glueContext,
    "machine_learning_curated_dyf"
)

#write parquet and update glue catalog
machine_learning_curated_sink = glueContext.getSink(
    path=target_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)
machine_learning_curated_sink.setFormat("glueparquet")
machine_learning_curated_sink.setCatalogInfo(
    catalogDatabase=database_name,
    catalogTableName=target_table
)
machine_learning_curated_sink.writeFrame(machine_learning_curated_dyf)

job.commit()