/* Exercise #2.
   Best/optimal. Aggregated second table in JOIN, unique customer_id
   1. Aggregate transaction with one row per customer_id
   2. JOIN with customers which also has one ro per customer_id (PRIMARY KEY)
   3. Group by customer age in decades
   4. Aggregate
 */
DROP TABLE per_age_per_dow;
CREATE TABLE per_age_per_dow(age VARCHAR(10), Mon INT, Tue INT, Wed INT, Thu INT, Fri INT, Sat INT, Sun INT)
WITH customers_id_visits_per_dow(customer_id, mon, tue, wed, thu, fri, sat, sun) AS (
    -- One row per customer_id
    SELECT customer_id,
           SUM(CASE WHEN DAYNAME(transaction_date)='Monday' THEN 1 ELSE 0 END) AS mon,
           SUM(CASE WHEN DAYNAME(transaction_date)='Tuesday' THEN 1 ELSE 0 END) AS tue,
           SUM(CASE WHEN DAYNAME(transaction_date)='Wednesday' THEN 1 ELSE 0 END) AS wed,
           SUM(CASE WHEN DAYNAME(transaction_date)='Thursday' THEN 1 ELSE 0 END) AS thu,
           SUM(CASE WHEN DAYNAME(transaction_date)='Friday' THEN 1 ELSE 0 END) AS fri,
           SUM(CASE WHEN DAYNAME(transaction_date)='Saturday' THEN 1 ELSE 0 END) AS sat,
           SUM(CASE WHEN DAYNAME(transaction_date)='Sunday' THEN 1 ELSE 0 END) AS sun
    FROM transactions
    GROUP BY customer_id ORDER BY customer_id
)
SELECT
    CONCAT(TIMESTAMPDIFF(YEAR, customer_dob, NOW()) DIV 10, '0+') AS age,
    SUM(mon) AS Mon, SUM(tue) AS Tue, SUM(wed) AS Wed, SUM(thu) AS Thu, SUM(fri) AS Fri, SUM(sat) AS Sat, SUM(sun) AS Sun
FROM customers JOIN customers_id_visits_per_dow ON customers.customer_id=customers_id_visits_per_dow.customer_id
GROUP BY age ORDER BY age;

SELECT * FROM per_age_per_dow;

/* Query execution order: 1. FROM, 2. JOIN, 3. WHERE, 4. GROUP BY, 5. HAVING, 6. SELECT, 7. ORDER BY, 8. LIMIT */
