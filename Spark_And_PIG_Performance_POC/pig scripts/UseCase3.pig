-- File: UseCase3.pig
-- Description:

-- For Running On Local System With Sample Data On Local File System
-- records = LOAD '/home/cloudera/project_input/trip_yellow_taxi.data' USING PigStorage(',') AS (VendorID:chararray, TPEP_PickUp_DateTime:chararray, TPEP_DropOff_DateTime:chararray, Passenger_Count:chararray, Trip_Distance:chararray, RateCodeID:chararray, Store_And_Forward_Flag:chararray, PULocationID:chararray, DOLocationID:chararray, Payment_Type:int, Fare_Amount:chararray, Extra:chararray, MTA_Tax:chararray, Tip_Amount:chararray, Tolls_Amount:chararray, Improvement_Surcharge:chararray, Total_Amount:chararray);

-- For Running On Local System With Big Data On Local File System
records = LOAD '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/data/big data/yellow_tripdata_*' USING PigStorage(',') AS (VendorID:chararray, TPEP_PickUp_DateTime:chararray, TPEP_DropOff_DateTime:chararray, Passenger_Count:chararray, Trip_Distance:chararray, RateCodeID:chararray, Store_And_Forward_Flag:chararray, PULocationID:chararray, DOLocationID:chararray, Payment_Type:int, Fare_Amount:chararray, Extra:chararray, MTA_Tax:chararray, Tip_Amount:chararray, Tolls_Amount:chararray, Improvement_Surcharge:chararray, Total_Amount:chararray);

filtered_records = FILTER records BY NOT (VendorID == 'VendorID');

filtered_data = FILTER filtered_records BY NOT (VendorID == '');

required_records = FOREACH filtered_data GENERATE Payment_Type AS Payment_Type, 1L AS CNT;

grouped_data = GROUP required_records BY Payment_Type;

final_records = FOREACH grouped_data GENERATE FLATTEN(group), SUM(required_records.CNT) AS CNT;

final_records_sorted = ORDER final_records BY CNT;

STORE final_records_sorted INTO '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/output/pig/usecase3.output';
