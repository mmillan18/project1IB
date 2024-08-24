-- TODO: This query will return a table with the differences between the real 
-- and estimated delivery times by month and year. It will have different 
-- columns: month_no, with the month numbers going from 01 to 12; month, with 
-- the 3 first letters of each month (e.g. Jan, Feb); Year2016_real_time, with 
-- the average delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_real_time, with the average delivery time per month of 2017 (NaN if 
-- it doesn't exist); Year2018_real_time, with the average delivery time per 
-- month of 2018 (NaN if it doesn't exist); Year2016_estimated_time, with the 
-- average estimated delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_estimated_time, with the average estimated delivery time per month 
-- of 2017 (NaN if it doesn't exist) and Year2018_estimated_time, with the 
-- average estimated delivery time per month of 2018 (NaN if it doesn't exist).
-- HINTS
-- 1. You can use the julianday function to convert a date to a number.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Take distinct order_id.

WITH delivery_times AS (
    SELECT 
        strftime('%Y', order_delivered_customer_date) AS year,
        strftime('%m', order_delivered_customer_date) AS month_no,
        strftime('%Y-%m', order_delivered_customer_date) AS year_month,
        julianday(order_delivered_customer_date) - julianday(order_estimated_delivery_date) AS delivery_difference
    FROM 
        olist_orders
    WHERE 
        order_status = 'delivered' 
        AND order_delivered_customer_date IS NOT NULL
    GROUP BY 
        order_id
),
real_times AS (
    SELECT 
        year_month,
        month_no,
        AVG(delivery_difference) AS avg_real_time
    FROM 
        delivery_times
    GROUP BY 
        year_month
),
estimated_times AS (
    SELECT 
        strftime('%Y', order_purchase_timestamp) AS year,
        strftime('%m', order_purchase_timestamp) AS month_no,
        strftime('%Y-%m', order_purchase_timestamp) AS year_month,
        AVG(julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp)) AS avg_estimated_time
    FROM 
        olist_orders
    WHERE 
        order_status = 'delivered' 
        AND order_delivered_customer_date IS NOT NULL
    GROUP BY 
        year_month
)
SELECT 
    month_no,
    strftime('%b', '2000-' || month_no || '-01') AS month,
    AVG(CASE WHEN strftime('%Y', year_month) = '2016' THEN avg_real_time END) AS Year2016_real_time,
    AVG(CASE WHEN strftime('%Y', year_month) = '2017' THEN avg_real_time END) AS Year2017_real_time,
    AVG(CASE WHEN strftime('%Y', year_month) = '2018' THEN avg_real_time END) AS Year2018_real_time,
    AVG(CASE WHEN strftime('%Y', year_month) = '2016' THEN avg_estimated_time END) AS Year2016_estimated_time,
    AVG(CASE WHEN strftime('%Y', year_month) = '2017' THEN avg_estimated_time END) AS Year2017_estimated_time,
    AVG(CASE WHEN strftime('%Y', year_month) = '2018' THEN avg_estimated_time END) AS Year2018_estimated_time
FROM 
    real_times
LEFT JOIN 
    estimated_times USING (year_month, month_no)
GROUP BY 
    month_no
ORDER BY 
    month_no;
