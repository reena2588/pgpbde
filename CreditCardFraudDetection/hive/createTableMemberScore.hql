CREATE EXTERNAL TABLE IF NOT EXISTS default.member_score (
       member_id STRING,
       score STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/input/data/tables/member_score';
