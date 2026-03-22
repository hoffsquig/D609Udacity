CREATE EXTERNAL TABLE stedi.accelerometer_landing (
    user STRING,
    timestamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-hoffman/accelerometer_landing/';