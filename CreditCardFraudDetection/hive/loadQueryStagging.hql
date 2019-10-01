INSERT OVERWRITE TABLE query_stagging 
SELECT
tmp_table.card_id,
(AVG(cast(tmp_table.amount AS DOUBLE))+3*STDDEV(cast(tmp_table.amount AS DOUBLE))) AS UCL
FROM (
SELECT
card_id,
amount,
transaction_dt,
RANK() OVER 
(
PARTITION BY card_id 
ORDER BY transaction_dt DESC
) AS Rank
FROM card_transactions
) tmp_table WHERE Rank <= 10 GROUP BY tmp_table.card_id;
