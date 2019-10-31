CREATE EXTERNAL TABLE IF NOT EXISTS India_Annual_Health_Survey_2012_13_DB.IAHS_2012_13_PARTITIONED_ORC_FORMAT(
ID SMALLINT,
State_District_Name STRING,
AA_Households_Total DOUBLE,
AA_Population_Total DOUBLE,
CC_Sex_Ratio_All_Ages_Total DOUBLE,
LL_Total_Fertility_Rate_Total DOUBLE,
YY_Under_Five_Mortality_Rate_U5MR_Total_Person DOUBLE)
PARTITIONED BY (State_Name String)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE India_Annual_Health_Survey_2012_13_DB.IAHS_2012_13_PARTITIONED_ORC_FORMAT PARTITION(State_Name)
SELECT
ID,
State_District_Name,
AA_Households_Total,
AA_Population_Total,
CC_Sex_Ratio_All_Ages_Total,
LL_Total_Fertility_Rate_Total,
YY_Under_Five_Mortality_Rate_U5MR_Total_Person,
State_Name
FROM India_Annual_Health_Survey_2012_13_DB.IAHS_2012_13;
