LOAD DATA INPATH '/user/hive/card_transactions.csv' OVERWRITE INTO TABLE stagging;  


INSERT OVERWRITE TABLE card_transactions 
SELECT named_struct('card_id',card_id, 'transaction_dt',transaction_dt, 'amount',amount), 
card_id, member_id, amount, postcode, pos_id, transaction_dt, status FROM stagging;
