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

WITH DeliveredOrders AS (
    SELECT 
        strftime('%Y', order_purchase_timestamp) AS year,
        strftime('%m', order_purchase_timestamp) AS month_no,
        strftime('%Y-%m', order_purchase_timestamp) AS year_month,
        CAST(
            julianday(order_delivered_customer_date) - 
            julianday(order_estimated_delivery_date) 
        AS INTEGER) AS delivery_diff,
        CAST(
            julianday(order_delivered_customer_date) - 
            julianday(order_purchase_timestamp) 
        AS INTEGER) AS real_delivery_time,
        order_status
    FROM 
        olist_orders
    WHERE 
        order_status = 'delivered'
        AND order_delivered_customer_date IS NOT NULL
)
SELECT 
    month_no,
    strftime('%b', '2000-' || month_no || '-01') AS month,
    AVG(CASE WHEN year = '2016' THEN real_delivery_time END) AS Year2016_real_time,
    AVG(CASE WHEN year = '2017' THEN real_delivery_time END) AS Year2017_real_time,
    AVG(CASE WHEN year = '2018' THEN real_delivery_time END) AS Year2018_real_time,
    AVG(CASE WHEN year = '2016' THEN delivery_diff END) AS Year2016_estimated_time,
    AVG(CASE WHEN year = '2017' THEN delivery_diff END) AS Year2017_estimated_time,
    AVG(CASE WHEN year = '2018' THEN delivery_diff END) AS Year2018_estimated_time
FROM 
    DeliveredOrders
GROUP BY 
    month_no;
