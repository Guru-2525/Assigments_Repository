
use classicmodels ;


select  a.orderNumber,
b.productName,b.productline
from orderdetails as a
left join products  b on  a.productCode=b.productCode ;

select a.customerNumber,
b.customerName ,
sum(a.amount) as 'amount spent',
b.country
from payments as a
left join customers as b on a.customerNumber=b.customerNumber 
group by customerNumber ;


select distinct a.country,count(a.customerName) as 'no of customers',a.salesRepEmployeeNumber,
sum(b.amount) as 'total sales',
concat(c.firstName," ",c.lastName) as 'Rep_Name'
from customers as a
left join payments as b on a.customerNumber=b.customerNumber
left join employees as c on a.salesRepEmployeeNumber=c.employeeNumber
group by country  
order by country ;


select 
concat(firstName,' ',lastName) as 'Sales Rep',
e.productLine,
sum(a.amount) as 'sales'
from payments as a
left join customers as b on a.customerNumber=b.customerNUmber
left join orders as c on a.customerNumber=c.customerNumber
left join orderdetails as d on c.orderNumber=d.orderNumber
left join products as e on e.productCode=d.productCode
left join employees as f on b.salesRepEmployeeNumber=f.employeeNumber
group by salesRepEmployeeNumber,productLine
order by concat(firstName,' ',lastName) ;


select a.customerName,
sum(b.amount) as "purchase amount",
(sum(MSRP)-sum(e.buyPrice)) as profit
from customers as a
left join payments as b on a.customerNumber=b.customerNumber
left join orders as c on a.customerNumber=c.customerNumber
left join orderdetails as d on c.orderNumber=d.orderNumber
left join products as e on d.productCode=e.productCode
where customerName like "A%"
group by customerName ;



select  productLine ,(sum(MSRP)-sum(buyPrice)) as profit ,sum(quantityInStock) as stock
from products  
group by productLine ;


select distinct a.customerName ,a.creditLimit,
sum(b.amount) as"purchased amount",(a.creditLimit-sum(b.amount)) as "Remaining amount",
round((((a.creditLimit-sum(b.amount))/sum(b.amount))*100),2) as "deviation in %"
from customers as a
left join payments as b on a.customerNumber=b.customerNumber
group by customerName ;

select distinct a.salesRepEmployeeNumber,a.customerNumber,
sum(b.amount) as"total sales"
from customers as a
left join payments as b on a.customerNumber=b.customerNumber                                           
  group by a.salesRepEmployeeNumber,b.customerNumber ;  
  
  
  
select a.productName,(a.MSRP-a.buyPrice) as "profit for each item",(a.quantityInStock*(a.MSRP-a.buyPrice)) as "profit of stock",
(a.quantityInStock-sum(quantityOrdered)) as "estimation for next year"
from products as a
left join orderdetails as b on a.productCode=b.productCode 
group by a.productName ;


select concat(a.contactFirstname," ",a.contactLastName) as "contact full name",
a.country,
concat(b.firstName," ",lastName) as "sales  Rep",
a.creditLimit,
sum(c.amount) as "total purchase amount",
group_concat(distinct(f.productLine)) as "product line bought"
from customers as a 
left join employees as b on a.salesRepEmployeeNumber=b.employeeNumber 
left join payments as c on a.customerNumber=c.customerNumber
left join orders as d on a.customerNumber=d.customerNumber
left join orderdetails as e on d.orderNumber=e.orderNumber
left join products as f on e.productCode=f.productCode
group by  concat(a.contactFirstname," ",a.contactLastName) ;






