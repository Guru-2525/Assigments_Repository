---------------------------------- SQL-ASSIGNMENT-2 ---------------------------------------------
	          ------------- Task 1:- Understanding the Data -----------------
       --  Describe the data in hand in your own words.-- 
   /* The superstores database contains 5(five) tables .
   They are :-
              1.cust_dimen
              2.market_fact
              3.orders_dimen
              4.prod_dimen
              5.shipping_dimen
     
 --1-Table:- cust_dimen contains these coloums 
 Columns:   Customer_Name (text)
			Province (text) 
            Region (text)
            Customer_Segment (text) 
            Cust_id (text)
THIS TABLE GIVES THE INFORMATION OF CUSTOMER LIKE ADDRESS ,HIS ID etc .           

--2-Table:-  market_fact
Columns:    Ord_id - text 
			Prod_id - text 
            Ship_id - text 
            Cust_id - text 
            Sales -  double 
            Discount - double 
            Order_Quantity - int 
            Profit - double 
            Shipping_Cost - double 
			Product_Base_Margin - double
THIS TABLE GIVES THE SALES OF PRODUCT AND PROFIT ,DISCOUNT ,QUANTITY AND ITS ID ALSO ,AND WE CAN OBSERVE THAT THIS IS THE MAIN TABLE BECAUSE IT CAN CONNECT TO ALL THE REMAINING TABLES.

--3-Table:-  orders_dimen
Columns:    Order_ID - int 
            Order_Date - text 
            Order_Priority - text 
            Ord_id - text
THIS TABLE GIVES THE ORDER INFORMATION LIKE ITS DATE ,PRIORITY AND ID .

--4-Table:-  prod_dimen
Columns:    Product_Category - text 
            Product_Sub_Category - text 
            Prod_id - text
THIS TABLES TELLS US WHICH PRODUCT BELONGS TO WHICH CATEGORY AND SUB_CATEGORY .

--5.Table:-   shipping_dimen
Columns:     Order_ID int 
             Ship_Mode text 
             Ship_Date text 
             Ship_id text
THIS TABLE TELLS US SHIPPING DETAILS LIKE WHAT IS THE SHIP DATE OF A PARTICULAR ORDER_ID ,MODE OF TRANSPORT .

*/
-- 2. Identify and list the Primary Keys and Foreign Keys for this dataset provided to you(In case you don’t find either primary or foreign key, then specially mention this in your answer) ------
/*
   TABLE-1:- cust_dimen
               Primary Key - CUST_ID
			   Foreign Key -
   TABLE-2:- market_fact
			   Primary Key -
			   Foreign Key - ORD_ID ,PROD_ID ,SHIP_ID ,CUST_ID
   TABLE-3:- orders_dimen
			   Primary Key - ORD_ID
			   Foreign Key -
   TABLE-4:- prod_dimen
			   Primary Key - PROD_ID
			   Foreign Key -
   TABLE-5:- shipping_dimen
               Primary Key - SHIP_ID
			   Foreign Key -  

 /* ------------------------------------- Task 2:- Basic & Advanced Analysis ----------------------------- */
 
 

USE SUPERSTORES ;

/***1. Write a query to display the Customer_Name and Customer Segment using alias name “Customer Name", "Customer Segment" from table Cust_dimen.**/


select customer_name as 'Customer Name',customer_segment as 'Customer Segment'
from cust_dimen;


/**** 2. Write a query to find all the details of the customer from the table cust_dimen order by desc.**/
 
 
select * from cust_dimen
order by customer_name desc ;

/*3. Write a query to get the Order ID, Order date from table orders_dimen where ‘Order Priority’ is high.************/

select order_id,order_date
from orders_dimen
where order_priority='high';

/***4. Find the total and the average sales (display total_sales and avg_sales) **/

select sum(sales) as total_sales ,avg(sales) as avg_sales
from market_fact ;

/**********5. Write a query to get the maximum and minimum sales from maket_fact table.**********/

select max(sales) as 'max sales',min(sales) as 'min sales'
from market_fact ;

/***6. Display the number of customers in each region in decreasing order of no_of_customers. The result should contain columns Region, no_of_customers.**/

select region,count(customer_name) as 'no_of_customers'
from cust_dimen
group by region
order by count(customer_name) desc ;

/*****7. Find the region having maximum customers (display the region name and max(no_of_customers)********/

select region,count(customer_name) as 'max no of customers'
from cust_dimen 
group by region
order by count(customer_name) desc
limit 1 ;

/******************8. Find all the customers from Atlantic region who have ever purchased ‘TABLES’ 
and the number of tables purchased (display the customer name, no_of_tables purchased)***********/

select a.customer_name,
count(b.order_quantity) as 'no_of_tables purchased'
from cust_dimen a
left join
market_fact b on a.cust_id=b.cust_id
where region='atlantic'
and prod_id= 'prod_11'
group by customer_name ;

/***9. Find all the customers from Ontario province who own Small Business. (display the customer name, no of small business owners)***/

select customer_name,count(customer_segment) as 'no of small business owners'
from cust_dimen
where province='ontario'
and customer_segment='small business'
group by customer_name ;

/****10. Find the number and id of products sold in decreasing order of products sold (display product id, no_of_products sold) *****/

select prod_id,sum(order_quantity) as 'no of products sold'
from market_fact
group by prod_id 
order by sum(order_quantity) desc ;

/*****11. Display product Id and product sub category whose produt category belongs to 
Furniture and Technlogy. The result should contain columns product id, product sub category.************/


select prod_id ,product_sub_category 
from prod_dimen
WHERE  product_category in  ('TECHNOLOGY' , 'FURNITURE') ;

/**12. Display the product categories in descending order of profits (display the product category wise profits i.e. product_category, profits)?***/

 select a.product_category,
 round(sum(b.profit),2) as 'sum of profit'
 from prod_dimen a
 left join 
 market_fact b on a.prod_id=b.prod_id
 group by product_category
 order by sum(b.profit) desc ;
 
 /****13. Display the product category, product sub-category and the profit within each subcategory in three columns. ********/

SELECT a.product_category,a.product_sub_category,
round(sum(b.profit),3) as profit 
from prod_dimen a
left join 
market_fact b on a.prod_id=b.prod_id
group by a.product_category,a.product_sub_category 
Order by a.Product_Category ;


/***14. Display the order date, order quantity and the sales for the order*****/

select a.order_date,
sum(b.order_quantity) as order_quantity ,round(sum(b.sales),3) as sales
from orders_dimen a
left join 
market_fact b on a.ord_id=b.ord_id 
group by order_date ;

/***********15. Display the names of the customers whose name contains the 
 i) Second letter as ‘R’
 ii) Fourth letter as ‘D’***************/
 
 /****    1       *****/
 
 select customer_name
 from cust_dimen
 where customer_name like '_r%' ;
 
 /******     2         ********/

 select customer_name
 from cust_dimen
 where customer_name like '___d%' ;
 
 
 /****16. Write a SQL query to to make a list with Cust_Id, Sales, Customer Name and 
their region where sales are between 1000 and 5000.*****/


select a.cust_id,a.customer_name,a.region,
b.sales
from cust_dimen a
left join
market_fact b on a.cust_id=b.cust_id 
where b.sales between  1000  and 5000 ;


/***17. Write a SQL query to find the 3rd highest sales.******/

select sales
from market_fact
order by sales desc
limit 1 offset 2 ;

/******18. Where is the least profitable product subcategory shipped the most? For the least 
profitable product sub-category, display the region-wise no_of_shipments and the profit made in each region in decreasing order of profits (i.e. region, 
no_of_shipments, profit_in_each_region)
 → Note: You can hardcode the name of the least profitable product subcategory******/

select c.region  ,
count(distinct b.ship_id) as 'countof id' , 
round(sum(a.profit),3) as 'profit'
from market_fact a
inner join 
shipping_dimen b  on a.Ship_id=b.ship_id 
inner join
cust_dimen c on a.cust_id=c.cust_id 
inner join
prod_dimen d on a.Prod_id=d.prod_id 
 where 
 d.product_sub_category   in                           
                             (
                             select
                             d.product_sub_category 
                             from market_fact a
							 inner join 
							 prod_dimen d on a.Prod_id=d.prod_id
                             group by d.product_sub_category
							 having sum(a.profit) <= all
                                                          (
                                                            select 
                                                            sum(a.profit)
															from market_fact a
                                                            inner join 
                                                            prod_dimen d on a.prod_id=d.Prod_id
														    group by d.Product_Sub_Category
                                                           )
 
						    )
 group by c.region
 order by count(distinct b.ship_id) desc ;
    
    
      ------------------------------ end -------------------------------
 
 

select
                             d.product_sub_category 
                             from market_fact a
							 inner join 
							 prod_dimen d on a.Prod_id=d.prod_id
                             group by d.product_sub_category
							 having sum(a.profit) <= all
                                                          (
                                                            select 
                                                            sum(a.profit)
															from market_fact a
                                                            inner join 
                                                            prod_dimen d on a.prod_id=d.Prod_id
														    group by d.Product_Sub_Category
                                                           )







    
    
    
   

