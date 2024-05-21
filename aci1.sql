/* Exercise #1.
   Optimal solution, JOINs smallest possible tables
   1. transactions table is compressed by aggregating everything in one row per customer_id
   2. offers table is compressed by aggregating one row per customer_id
   3. now all three tables have one row per customer-id and can be economically JOINed
 */
WITH
    transactions_summary(customer_id, redeemed, total_sales, n_sales, Mon, Tue, Wed, Thu, Fri, Sat, Sun) AS ( -- One row per customer_id
        SELECT
            customer_id,
            COUNT(offer_id) AS redeemed,  -- if offer_id IS NULL, purchase without offer
            SUM(sales_amount) AS total_sales,
            COUNT(*) AS n_sales,  -- >= offers_redeemed, transactions on all days, unneeded, good for control
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Monday' THEN 1 ELSE 0 END)    AS Mon,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Tuesday' THEN 1 ELSE 0 END)   AS Tue,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Wednesday' THEN 1 ELSE 0 END) AS Wed,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Thursday' THEN 1 ELSE 0 END)  AS Thu,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Friday' THEN 1 ELSE 0 END)    AS Fri,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Saturday' THEN 1 ELSE 0 END)  AS Sat,
            SUM(CASE WHEN DAYNAME(transaction_date) = 'Sunday' THEN 1 ELSE 0 END)    AS Sun
        FROM transactions GROUP BY customer_id ORDER BY customer_id
    ),
    offers_summary(customer_id, received) AS (  -- One row per customer_id, count offers_received
        SELECT
            offers.customer_id AS customer_id,
            COUNT(offer_id) AS received
        FROM offers  -- JOIN customers ON offers.customer_id=customers.customer_id
        GROUP BY customer_id ORDER BY customer_id
    )
SELECT
    customer_name AS name,
    customers.customer_id AS id,
    TIMESTAMPDIFF(YEAR, customer_dob, NOW()) AS age,
    total_sales AS tot_sales,
    received, redeemed,
    n_sales, Mon, Tue, Wed, Thu, Fri, Sat, Sun  -- n_sales per dow
FROM
    transactions_summary JOIN offers_summary ON transactions_summary.customer_id=offers_summary.customer_id
        JOIN customers ON customers.customer_id=transactions_summary.customer_id
ORDER BY id;

/* Query execution order: 1. FROM, 2. JOIN, 3. WHERE, 4. GROUP BY, 5. HAVING, 6. SELECT, 7. ORDER BY, 8. LIMIT */
