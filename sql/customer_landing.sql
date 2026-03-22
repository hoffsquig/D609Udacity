CREATE EXTERNAL TABLE stedi.customer_landing (
    customerName STRING,
    email STRING,
    phone STRING,
    birthDay STRING,
    serialNumber STRING,
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-hoffman/customer_landing/';