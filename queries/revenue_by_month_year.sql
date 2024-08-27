-- TODO: This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

WITH filtered_orders AS (
    SELECT
        oo.order_delivered_customer_date AS order_del,
        oop.payment_value as payment_val,
        ROW_NUMBER() OVER (PARTITION BY oop.order_id) AS filtering
    FROM olist_orders oo
    JOIN olist_order_payments oop ON oop.order_id = oo.order_id
    WHERE 
        oo.order_status = 'delivered' 
        AND oo.order_delivered_customer_date IS NOT NULL
), revenue_by_month_year AS (
	SELECT
	strftime('%m', fo.order_del) AS month_no,
	strftime('%Y', fo.order_del) AS year,
	fo.payment_val AS payment
	FROM 
		filtered_orders fo
	WHERE 
    	fo.filtering = 1
)
SELECT 
    rm.month_no,
    CASE
        WHEN (rm.month_no) = '02' THEN 'Feb'
        WHEN (rm.month_no) = '03' THEN 'Mar'
        WHEN (rm.month_no) = '04' THEN 'Apr'
        WHEN (rm.month_no) = '05' THEN 'May'
        WHEN (rm.month_no) = '06' THEN 'Jun'
        WHEN (rm.month_no) = '07' THEN 'Jul'
        WHEN (rm.month_no) = '08' THEN 'Aug'
        WHEN (rm.month_no) = '09' THEN 'Sep'
        WHEN (rm.month_no) = '10' THEN 'Oct'
        WHEN (rm.month_no) = '11' THEN 'Nov'
        WHEN (rm.month_no) = '12' THEN 'Dec'
    END AS month,
    SUM(CASE WHEN rm.year = '2016' THEN rm.payment ELSE 0.00 END) AS Year2016,
    SUM(CASE WHEN rm.year = '2017' THEN rm.payment ELSE 0.00 END) AS Year2017,
    SUM(CASE WHEN rm.year = '2018' THEN rm.payment ELSE 0.00 END) AS Year2018
FROM 
    revenue_by_month_year rm
GROUP BY 
   rm.month_no
ORDER BY 
   rm.month_no;