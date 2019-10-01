CREATE EXTERNAL TABLE IF NOT EXISTS default.card_member (
	card_id STRING,
	member_id STRING,
	member_joining_dt STRING,
	card_purchase_dt STRING,
	country STRING,
	city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/input/data/tables/card_member';

