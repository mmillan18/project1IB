-- TODO: This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

WITH MonthlyRevenue AS (
    SELECT 
        strftime('%Y', order_purchase_timestamp) AS year,
        strftime('%m', order_purchase_timestamp) AS month_no,
        SUM(payment_value) AS total_revenue
    FROM 
        olist_orders
    JOIN 
        olist_order_payments 
    ON 
        olist_orders.order_id = olist_order_payments.order_id
    WHERE 
        strftime('%Y', order_purchase_timestamp) IN ('2016', '2017', '2018')
    GROUP BY 
        year, month_no
)
SELECT 
    month_no,
    strftime('%b', '2000-' || month_no || '-01') AS month,
    SUM(CASE WHEN year = '2016' THEN total_revenue ELSE 0 END) AS Year2016,
    SUM(CASE WHEN year = '2017' THEN total_revenue ELSE 0 END) AS Year2017,
    SUM(CASE WHEN year = '2018' THEN total_revenue ELSE 0 END) AS Year2018
FROM 
    MonthlyRevenue
GROUP BY 
    month_no;
