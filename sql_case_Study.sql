create database  case_study;
use case_study;

-- sales performance over time 

select date_format(order_date,'%Y-%m') as year,
sum(sales_amount) as total_sales
 from sales 
 where date_format(order_date,'%Y-%m') is not null
 GROUP BY date_format(order_date,'%Y-%m')
 order by date_format(order_date,'%Y-%m')
 ;
-- cumulative anylysiis 
select order_dates,total_sales_month,
sum(total_sales_month) OVER ( ORDER BY order_dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  ) as cumulative_sales
FROM(
select date_format(order_date,'%Y-%m') as order_dates,sum(sales_amount) as total_sales_month from sales 
where date_format(order_date,'%Y-%m')  is not null
group by date_format(order_date,'%Y-%m') 
) as sub ;

-- performance anylysis  year on year sales 
with cte as (SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM sales f
LEFT JOIN products p
ON f.product_key = p.product_key
WHERE YEAR(f.order_date)  IS NOT NULL
GROUP BY
YEAR(f.order_date),
p.product_name
)

SELECT 
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change
    ,lag(current_sales) over(Partition by product_name order by order_year) as previous_year_sales
,case when (current_sales - lag(current_sales) over(Partition by product_name order by order_year)) > 0 then 'Increase'
when (current_sales - lag(current_sales) over(Partition by product_name order by order_year)) < 0 then 'Decrease'
 Else 'NO CHANGE'
 END as previous_change
 FROM cte
ORDER BY product_name, order_year;


-- part to hole analysis 
WITH category_sales AS (
    SELECT 
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM sales f
    LEFT JOIN products p
        ON f.product_key = p.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    concat(round((total_sales / SUM(total_sales) OVER ()) * 100,2),'%') AS percentage_of_total
FROM category_sales;
-- only one category reliaing on one category is ganderous as al business is relied on this 

-- data segmentation 

WITH product_segments AS (
  SELECT
    product_key,
    product_name,
    cost,
    CASE 
      WHEN cost < 100 THEN 'Below 100'
      WHEN cost BETWEEN 100 AND 500 THEN '100-500'
      WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
      ELSE 'Above 1000'
    END AS cost_range
  FROM products
)
SELECT
  cost_range,
  COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products desc;



SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF (MIN(order_date), MAX(order_date)) AS lifespan
FROM sales as s 
LEFT JOIN customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key;



-- report 
create view  final_customer_data AS
WITH base_query AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        ROUND(ABS(DATEDIFF(c.birthdate, '2025-05-09') / 365.0), 0) AS age
    FROM sales f
    LEFT JOIN customers c
        ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),
customer_aggregation AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        ROUND(ABS(DATEDIFF(MIN(order_date), MAX(order_date))), 0) AS lifespan
    FROM base_query
    GROUP BY
        customer_key,
        customer_number,
        customer_name,
        age
)
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,CASE
WHEN age < 20 THEN 'Under 20'
WHEN age between 20 and 29 THEN '20-29'
WHEN age between 30 and 39 THEN '30-39'
WHEN age between 40 and 49 THEN '40-49'
ELSE '50 and abape above'
END AS age_group,
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    case when total_orders = 0 then 0
    else  total_sales/total_orders 
    END as avg_order_value
FROM customer_aggregation;





-- product report data 
Create View product_agg_report as 
WITH product_base_query AS (
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM sales f
    LEFT JOIN products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),
product_aggregations AS (
    -- Summarizes key metrics at the product level
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders, 
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
    FROM product_base_query
    GROUP BY 
        product_key, product_name, category, subcategory, cost
)
SELECT *
FROM product_aggregations;


