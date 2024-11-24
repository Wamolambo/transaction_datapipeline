CREATE OR REPLACE VIEW transactions_per_productcategory AS
SELECT product_category, 
       COUNT(*) AS total_transactions
FROM planet42.p42_transactions
GROUP BY product_category;
