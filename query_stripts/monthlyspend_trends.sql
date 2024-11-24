CREATE OR REPLACE VIEW monthlyspend_trends AS
SELECT 
    MONTH(transaction_date) AS month, 
    SUM(transaction_amount) AS total_spend
FROM transactions
GROUP BY month;
