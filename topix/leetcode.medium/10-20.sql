--Countries you can safely invest in.sql
with t as(select *, left(phone_number,3) as cc
from Person),
t2 as (
select callee_id, duration
from Calls
union 
select caller_id,duration
from Calls)
select * from (
select distinct c.name, avg(duration) su
from t left join t2
on t.id = t2.callee_id
join Country c
on c.country_code = t.cc
group by c.name) r
where su > ( select avg(duration) from t2)
--Customers who bought a, b but not c.sql Customers72 orders72
select distinct c.customer_id, customer_name
from orders72 o join Customers72 c
on o.customer_id =c.customer_id 
where c.customer_id not in( select customer_id from Orders72 where product_name = 'C')
and c.customer_id in ( select customer_id from Orders72 where product_name = 'A')
and c.customer_id in (select customer_id from Orders72 where product_name = 'B')
--Customers who bought all products.sql Customer93
select customer_id
from Customer93
group by customer_id
having COUNT(distinct product_key) = (select COUNT(distinct product_key) from Product93) 
--Department Highest Salary.sql Employee2 department1
with t as (
select *, DENSE_RANK() over(partition by departmentid order by salary desc) rk
from Employee2)
select d.Name as department, t.Name employee , Salary
from department1 d join t 
on t.DepartmentId = d.Id
where rk = 1
--Evaluate Boolean Expressions.sql Variables Expressions
with t as (select e.left_operand,e.operator,e.right_operand,v.value as leftval,v1.value as rightval
from Expressions e join Variables v
on e.left_operand = v.name 
join Variables v1
on e.right_operand = v1.name)
select left_operand,operator,right_operand,case when operator = '>' and t.leftval > t.rightval then 'True'
when operator = '<' and t.leftval<t.rightval then 'True'
when operator = '=' and t.leftval = t.rightval then 'True'
else 'False'
end as a
from t
