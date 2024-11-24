CREATE OR REPLACE VIEW top5_transactions AS
SELECT 
	customer_id, 
	SUM(transaction_amount) AS total_value
FROM planet42.p42_transactions
GROUP BY customer_id
ORDER BY total_value DESC
LIMIT 5;
