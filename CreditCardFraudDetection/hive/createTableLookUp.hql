CREATE EXTERNAL TABLE IF NOT EXISTS default.look_up (
	card_id STRING, 
	member_id STRING,
	member_joining_dt STRING,
	card_purchase_dt STRING,
	country STRING,
	city STRING,
	UCL STRING, 
	postcode STRING, 
	transaction_dt STRING,
	score STRING
)
ROW FORMAT DELIMITED 
COLLECTION ITEMS TERMINATED BY '~' 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf1:member_id,cf1:member_joining_dt,cf1:card_purchase_dt,cf1:country,cf1:city,cf1:UCL,cf1:postcode,cf1:transaction_dt,cf1:score")
TBLPROPERTIES ("hbase.table.name" = "look_up");
