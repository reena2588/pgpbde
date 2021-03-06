-- File: UseCase2.pig
-- Description:

-- For Running On Local System With Sample Data On Local File System
-- records = LOAD '/home/cloudera/project_input/trip_yellow_taxi.data' USING PigStorage(',') AS (VendorID, TPEP_PickUp_DateTime, TPEP_DropOff_DateTime, Passenger_Count, Trip_Distance, RateCodeID:int, Store_And_Forward_Flag, PULocationID, DOLocationID, Payment_Type, Fare_Amount, Extra, MTA_Tax, Tip_Amount, Tolls_Amount, Improvement_Surcharge, Total_Amount);

-- For Running On Local System With Big Data On Local File System
records = LOAD '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/data/big data/yellow_tripdata_*' USING PigStorage(',') AS (VendorID, TPEP_PickUp_DateTime, TPEP_DropOff_DateTime, Passenger_Count, Trip_Distance, RateCodeID:int, Store_And_Forward_Flag, PULocationID, DOLocationID, Payment_Type, Fare_Amount, Extra, MTA_Tax, Tip_Amount, Tolls_Amount, Improvement_Surcharge, Total_Amount);

filtered_records = FILTER records BY NOT (VendorID == 'VendorID');

final_records = FILTER filtered_records BY (RateCodeID == 4);

STORE final_records INTO '/home/cloudera/workspace/Spark_And_PIG_Performance_POC/output/pig/usecase2.output';
