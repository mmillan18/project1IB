-- TODO: This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

WITH revenue_by_month_year AS (
    SELECT 
        strftime('%Y', o.order_purchase_timestamp) AS year,
        strftime('%m', o.order_purchase_timestamp) AS month_no,
        SUM(p.payment_value) AS revenue
    FROM 
        olist_orders o
    JOIN 
        olist_order_payments p ON o.order_id = p.order_id
    WHERE 
        o.order_status = 'delivered' 
        AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY
        year, month_no
)
SELECT 
    month_no,
    CASE
        WHEN month_no = '01' THEN 'Jan'
        WHEN month_no = '02' THEN 'Feb'
        WHEN month_no = '03' THEN 'Mar'
        WHEN month_no = '04' THEN 'Apr'
        WHEN month_no = '05' THEN 'May'
        WHEN month_no = '06' THEN 'Jun'
        WHEN month_no = '07' THEN 'Jul'
        WHEN month_no = '08' THEN 'Aug'
        WHEN month_no = '09' THEN 'Sep'
        WHEN month_no = '10' THEN 'Oct'
        WHEN month_no = '11' THEN 'Nov'
        WHEN month_no = '12' THEN 'Dec'
    END AS month,
    COALESCE(SUM(CASE WHEN year = '2016' THEN revenue ELSE 0.00 END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN year = '2017' THEN revenue ELSE 0.00 END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN year = '2018' THEN revenue ELSE 0.00 END), 0.00) AS Year2018
FROM 
    revenue_by_month_year
GROUP BY 
    month
ORDER BY 
    month_no