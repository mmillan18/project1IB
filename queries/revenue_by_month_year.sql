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
        strftime('%Y-%m', o.order_purchase_timestamp) AS year_month,
        SUM(p.payment_value) AS revenue
    FROM 
        olist_orders o
    JOIN 
        olist_order_payments p ON o.order_id = p.order_id
    WHERE 
        o.order_status = 'delivered' 
        AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY 
        year_month
)
SELECT 
    month_no,  
    CASE(
        WHEN month_no = '01' THEN 'Ene'
        WHEN month_no = '02' THEN 'Feb'
        WHEN month_no = '03' THEN 'Mar'
        WHEN month_no = '04' THEN 'Abr'
        WHEN month_no = '05' THEN 'May'
        WHEN month_no = '06' THEN 'Jun'
        WHEN month_no = '07' THEN 'Jul'
        WHEN month_no = '08' THEN 'Ago'
        WHEN month_no = '09' THEN 'Sep'
        WHEN month_no = '10' THEN 'Oct'
        WHEN month_no = '11' THEN 'Nov'
        WHEN month_no = '12' THEN 'Dic'
    ) as month, FROM revenue_by_month_year;