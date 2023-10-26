create database mini_project;
use mini_project;
select * from cust_dimen;
select * from market_fact;

-- 1.Join all the tables and create a new table called combined_table.(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)
create table combined_table as 
select m.*,c.customer_name,c.province,c.region,c.customer_segment,o.order_id,o.order_date,o.order_priority,p.product_category,p.product_sub_category,
 s.order_id ship_order_id, s.ship_mode,s.ship_date from market_fact m
inner join cust_dimen c
on m.cust_id=c.cust_id
inner join orders_dimen o
on m.ord_id=o.ord_id
inner join prod_dimen p
on m.prod_id=p.prod_id
inner join shipping_dimen s
on m.ship_id=s.ship_id;

-- 2.	Find the top 3 customers who have the maximum number of orders
select * from combined_table;

select customer_name,sum(order_quantity)  from
combined_table group by customer_name   order by sum(order_quantity)  desc limit 3;

-- 3.Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
create table ship_details as
select ord_id,customer_name,cust_id,ship_date,order_date,
datediff(str_to_date(ship_date,'%d-%m-%Y'),str_to_date(order_date,'%d-%m-%Y')) 
as DaysTakenForDelivery 
from combined_table;

select * from ship_details;

-- 4.	Find the customer whose order took the maximum time to get delivered.
select ord_id,customer_name,cust_id,
DaysTakenForDelivery from ship_details
where DaysTakenForDelivery =
(select max(DaysTakenForDelivery) from ship_details) ;

-- 5	Retrieve total sales made by each product from the data (use Windows function)
select * from combined_table;

select distinct prod_id,sum(sales)over(partition by prod_id ) as total_sales  from combined_table ;

-- 6 Retrieve total profit made from each product from the data (use windows function)
select distinct prod_id, sum(profit)over(partition by prod_id) from combined_table;

-- 7 	Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
select distinct cust_id,order_date
from combined_table
where year(str_to_date(order_date,'%d-%m-%Y'))=2011 
and month(str_to_date(order_date,'%d-%m-%Y'))=01;

create view customer as 
select cust_id,customer_name,
order_id,str_to_date(order_date,'%d-%m-%Y') as date_of_order from combined_table;

select * from customer;

select year(date_of_order) as year_of_order, 
month(date_of_order) as month_of_order, count(distinct cust_id) as Unique_customers
from customer
where year(date_of_order)=2011 
and customer_name in (select distinct customer_name from customer 
where year(date_of_order)=2011 and month(date_of_order) = 01)
group by month(date_of_order);

-- 8 	Retrieve month-by-month customer retention rate since the start of the business.(using views)
-- STEP 1

create view retention as 
select customer_name, month(str_to_date(order_Date,'%d-%m-%Y')) month_visit, count(cust_id) visit from combined_table 
group by customer_name, month(str_to_date(order_Date,'%d-%m-%Y'))
order by customer_name,month(str_to_date(order_Date,'%d-%m-%Y'));

select * from retention;

-- STEP 2

create view retention1 as 
select *, lead(visit) over(partition by customer_name) next_month_count, 
( (lead(month_visit) over(partition by customer_name)) - month_visit) `time_diff (in_month)` from retention ;


-- STEP 3

create view retention2 as 
select *,case 
			when `time_diff (in_month)` = 1 then 'retained'
            when `time_diff (in_month)` >1 then 'irregular'
            when `time_diff (in_month)` is null then 'churned'
		 end cust_categorise
from retention1;

select * from retention2;

-- STEP 4 

-- CUSTOMER_NAME 

create view retention3 as 
select customer_name, 
month(str_to_date(order_Date,'%d-%m-%Y')) month,
lead(month(str_to_date(order_Date,'%d-%m-%Y'))) over(partition by customer_name) next_month,
count(cust_id) visit, 
lead(count(cust_id)) over(partition by customer_name) next_v 
from combined_table 
group by customer_name, month(str_to_date(order_Date,'%d-%m-%Y'))
order by customer_name,month(str_to_date(order_Date,'%d-%m-%Y'));



select month, ( count(if( (next_month = (month + 1)) , customer_name,null)) /count(distinct customer_name) ) retente from retention3  group by month;
select month, ( count(if( (next_month = (month + 1)) , customer_name,null)) /count(customer_name) ) retente from retention3  group by month;


-- USING CUST_ID

create view retention4 as 
select customer_name, cust_id,
month(str_to_date(order_Date,'%d-%m-%Y')) month,
lead(month(str_to_date(order_Date,'%d-%m-%Y'))) over(partition by customer_name) next_month,
count(cust_id) visit, 
lead(count(cust_id)) over(partition by customer_name) next_v 
from combined_table 
group by cust_id, month(str_to_date(order_Date,'%d-%m-%Y'))
order by cust_id,month(str_to_date(order_Date,'%d-%m-%Y'));

select *from retention4;
select * from retention3;


select month, (count( distinct if( (next_month = (month + 1)) , cust_id,null)) /count(distinct cust_id) )*100 retente from retention4  group by month;
select month, ( count(if( (next_month = (month + 1)) , cust_id,null)) /count(cust_id) ) retente from retention4  group by month;






