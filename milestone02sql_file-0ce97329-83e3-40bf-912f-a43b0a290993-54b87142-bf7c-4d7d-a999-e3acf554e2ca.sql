-- Analyses I 

-- 1 
SELECT payment_type,CEIL(AVG(payment_value)) as rounded_avg_payment 
FROM payments group by 1 
having CEIL(AVG(payment_value)) != 0 
order by rounded_avg_payment;

-- 2
SELECT payment_type, 
       ROUND(COUNT(order_id) * 100.0 / (SELECT COUNT(order_id) FROM payments), 1) AS percentage
FROM payments
GROUP BY payment_type
ORDER BY percentage DESC ;

-- 3
SELECT t1.product_id,t2.price FROM product t1
LEFT JOIN order_items t2 ON t1.product_id = t2.product_id
WHERE t1.product_category_name LIKE '%smart%' AND t2.price BETWEEN 100 AND 500 
ORDER BY price DESC;

-- 4
SELECT 
to_char(t1.order_purchase_timestamp, 'month') as month,
ceil(SUM(payment_value)) as total_sales
FROM orders t1
LEFT JOIN payments t2 ON t1.order_id = t2.order_id
group by 1
order by total_sales desc limit 3;

-- 5
select * from(
SELECT product_category_name, (max(price) - min(price)) as price_difference FROM order_items t1
LEFT JOIN product t2 ON t1.product_id = t2.product_id
group by 1)t where t.price_difference>500 order by t.price_difference desc;

-- 6
select t2.payment_type,round(stddev(price::numeric),2) as std_deviation from order_items t1
join payments t2
on t1.order_id = t2.order_id
group by 1
order by stddev(price);

-- 1.7
select product_id,product_category_name from product
where  length(product_category_name) = 1 or product_category_name is null;

-- Analyses II

-- 1
with cte1 as (select t.order_value_segement, t.payment_type, count(*) from (
select *,
case 
	when price<200 then 'low'
	when price between 200 and 1000 then 'medium'
	when price>1000 then 'high'
	end as order_value_segement
from order_items t1 
join payments t2 on t1.order_id = t2.order_id)t
group by 1,2)

select t.order_value_segement, t.payment_type,t.count from (
select *,
dense_rank() over(partition by order_value_segement order by count desc) as rnk
from cte1)t
where t.rnk =1;

-- 2
select t2.product_category_name,max(t1.price) as max_price,
min(t1.price) as min_price, ROUND(avg(t1.price::NUMERIC),2) as avg_price from order_items t1
left join product t2
on t1.product_id = t2.product_id
group by 1  order by avg(t1.price) desc;

-- 3
select distinct customer_id,count(*) as total_orders from orders
group by 1  having count(*) > 1 order by count(*) desc;

-- 4
ALTER TABLE customer1 ADD COLUMN customer_type VARCHAR(50);

create temporary table  temp1(
customer_type varchar(50),
min_order int,
max_order int
)
insert into temp1
values('New',1,1),('Returning',2,4),('Loyal',5,null)

with cte1 as (select customer_id,count(*) as freq from orders 
group by 1)
UPDATE customer1
SET customer_type = temp1.customer_type
FROM cte1
LEFT JOIN temp1  -- Ensure we include all cases
    ON cte1.freq BETWEEN temp1.min_order AND COALESCE(temp1.max_order, cte1.freq)
WHERE customer1.customer_id = cte1.customer_id;

UPDATE customer1
SET customer_type = 'No Orders'
WHERE customer_type IS NULL;

select * from customer1;

-- 5
select t3.product_category_name,round(sum(t2.price::numeric),2) as total_revenue from orders t1
join order_items t2 on t1.order_id = t2.order_id 
join product t3 on t2.product_id = t3.product_id
group by 1 order by sum(t2.price) desc limit 5;

-- Analyses III

-- 1
with final_data as (
select *,
case when month_name in('March', 'April', 'May') then 'Spring'
when month_name in ('June','July', 'August' ) then 'Summer'
when month_name in ('September','October', 'November') then 'Autumn'
else 'Winter' end as Season
from (
select *, trim(initcap(to_char( order_purchase_timestamp, 'month'))) as month_name from orders t1 
join order_items t2 on t1.order_id = t2.order_id)t)

select season,round(sum(price::numeric),2)as total_sales from final_data
group by 1;

-- 2
with tab1 as 
(select product_id, count(*) as total_quantity_sold  from order_items
group by 1)

select * from tab1 
where total_quantity_sold > (select round(avg(total_quantity_sold)) from tab1);

-- 3
with temp1 as (
select *,
initcap(to_char(order_purchase_timestamp, 'month')) as months
from orders t1
where extract(year from order_purchase_timestamp) = 2018)

select months,round(sum(price::numeric),2) as total_revenue from temp1 t1
join order_items t2 on t1.order_id = t2.order_id
group by 1;

-- 4
with temp1 as (
select *,
case when pur_freq between 1 and 2 then 'Occasional'
when pur_freq between 3 and 5 then 'Regular'
when pur_freq> 5 then 'Loyal'
end as customer_type
from (
select customer_id,count(*) pur_freq from orders
group by 1))

select customer_type,count(*) from temp1
group by 1;

-- 5
select *,dense_rank() over(order by avg_order_value desc)  as customer_rank from (
select t1.customer_id,avg(price) as avg_order_value from orders t1
join order_items t2  on t1.order_id = t2.order_id
group by 1) limit 20;

-- 6
SELECT 
    product_id,
    DATE_TRUNC('month', order_purchase_timestamp) AS month_start,
    SUM(price) AS total_sales,
    SUM(SUM(price)) OVER (PARTITION BY product_id ORDER BY DATE_TRUNC('month', order_purchase_timestamp)) AS cumulative_sales
FROM orders t1 
JOIN order_items t2 ON t1.order_id = t2.order_id
JOIN product t3 ON t2.product_id = t3.product_id
GROUP BY product_id, month_start
ORDER BY product_id, month_start;

-- 7
with cte1 as (
select t.payment_type,t.months as sale_month ,
month_no,
round(sum(t.price::numeric),2) as  monthly_total
from (
select *,
initcap(to_char(order_purchase_timestamp,'month')) as months,
extract(month from order_purchase_timestamp ) as month_no
from orders t1
join order_items t2
on t1.order_id = t2.order_id
join payments t3 on t2.order_id = t3.order_id
where extract(year from order_purchase_timestamp ) = 2018)t
group by t.payment_type,t.months,month_no)

select payment_type,sale_month,monthly_total,monthly_change from(
select *,
((monthly_total - lag(monthly_total) over(partition by payment_type order by month_no))/
  lag(monthly_total) over(partition by payment_type order by month_no))*100.0 as monthly_change
from cte1);