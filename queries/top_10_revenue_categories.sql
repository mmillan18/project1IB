-- TODO: This query will return a table with the top 10 revenue categories in 
-- English, the number of orders and their total revenue. The first column will 
-- be Category, that will contain the top 10 revenue categories; the second one 
-- will be Num_order, with the total amount of orders of each category; and the 
-- last one will be Revenue, with the total revenue of each catgory.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.

SELECT 
    pt.product_category_name_english AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    SUM(COALESCE(op.payment_value, 0)) AS Revenue
FROM 
    olist_order_items oi
JOIN 
    olist_orders o ON o.order_id = oi."00""order_id"""
JOIN
    olist_products p ON p.product_id = oi.product_id
JOIN
    olist_order_payments op ON op.order_id = oi."00""order_id"""
JOIN
    product_category_name_translation pt ON pt.product_category_name = p.product_category_name
WHERE 
    o.order_status = 'delivered'
    AND p.product_category_name IS NOT NULL
    AND o.order_delivered_customer_date IS NOT NULL
GROUP BY 
    pt.product_category_name_english
ORDER BY 
    Revenue DESC
LIMIT 10;
