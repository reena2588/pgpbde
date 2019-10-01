INSERT OVERWRITE TABLE look_up
SELECT temp_table.card_id, card_member.member_id, card_member.member_joining_dt, card_member.card_purchase_dt, card_member.country, card_member.city, query_stagging.ucl, temp_table.postcode, temp_table.transaction_dt, member_score.score
FROM (
SELECT
card_id,
member_id,
transaction_dt,
postcode,
ROW_NUMBER() OVER 
(
PARTITION BY card_id
ORDER BY transaction_dt DESC
) AS Rank1
FROM card_transactions)
temp_table JOIN card_member ON (temp_table.card_id = card_member.card_id) JOIN query_stagging ON (temp_table.card_id = query_stagging.card_id) JOIN member_score ON (temp_table.member_id = member_score.member_id)
WHERE temp_table.Rank1 = 1
