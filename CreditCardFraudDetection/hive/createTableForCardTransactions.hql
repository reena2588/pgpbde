CREATE EXTERNAL TABLE IF NOT EXISTS default.card_transactions (
	key struct<card_id:STRING, transaction_dt:STRING, amount:STRING>,
	card_id STRING, 
	member_id STRING, 
	amount STRING,
	postcode STRING, 
	pos_id STRING, 
	transaction_dt STRING, 
	status STRING
)
ROW FORMAT DELIMITED 
COLLECTION ITEMS TERMINATED BY '~' 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:card_id,cf1:member_id,cf1:amount,cf1:postcode,cf1:pos_id,cf1:transaction_dt,cf1:status")
TBLPROPERTIES ("hbase.table.name" = "card_transactions","skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS default.stagging (
	card_id STRING, 
	member_id STRING, 
	amount STRING,
	postcode STRING, 
	pos_id STRING, 
	transaction_dt STRING, 
	status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
