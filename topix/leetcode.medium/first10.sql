--Active Businesses.sql tables events
with t as (select distinct event_type, avg(occurrences*1.0) avg 
from events
group by event_type)
select business_id 
from Events e join t
on t.event_type = e.event_type
where e.occurrences > t.avg
group by business_id
having count(*)>1 
--Active Users.sql tables Logins
with t as 
(select *, lead(login_date,1) over(partition by id order by login_date) as nx
from Logins )
select distinct t.id as active_user, name from t join Accounts a
on t.id = a.id
where DATEDIFF(day,login_date,nx) = 1 
group by t.id , name
having count(*) >= 4
--Activity Participants.sql tables Friends
with t as 
(select distinct activity, COUNT(*)   ss
from Friends 
group by activity), 
t1 as(
select max(ss) cc from t
union
select min(ss) from t)
select Activity from t 
where ss not in ( select cc from t1)x
--All people report to the given manager.sql tables employees
select employee_id
from employees
where manager_id = 1 and employee_id != 1
union
select employee_id
from employees
where manager_id = any (select employee_id
from employees
where manager_id = 1 and employee_id != 1)
union
select employee_id
from employees
where manager_id = any (select employee_id
from employees
where manager_id = any (select employee_id
from employees
where manager_id = 1 and employee_id != 1))
--Apples & Oranges.sql tables Sales55
with a as (select *
from Sales55
where fruit = 'apples'),
o as (select * 
from Sales55
where fruit = 'oranges')
select a.sale_date, (a.sold_num - o.sold_num) as diff
from a join o
on a.sale_date =o.sale_date
--Article Views 2.sql tables Views
select viewer_id as id
from Views
group by viewer_id,view_date
having COUNT(distinct article_id) > 1
order by id
--Calculate Salaries.sql tables Salaries
with t as (
select company_id,AVG(salary) avg
from Salaries
group by company_id)
select s.company_id,employee_id,employee_name, case when avg < 1000 then salary
when avg > 10000 then round(salary*0.51,0) 
else round(salary*0.76,0)
end as salary
from t join Salaries s
on t.company_id = s.company_id
--Capital Gain.sql tables Stocks
select distinct Stock_name, sum(case when operation = 'Buy' then -price
							else price
							end) as capital_gain_loss
from Stocks
group by stock_name
--Consecutive Numbers.sql tables logs
with t as (select *, LEAD(num,1) over(order by id) as nxt, LAG(num,1) over(order by id) prev
from Logs)
select num from t
where num = nxt and num = prev 
--Count student number in departments.sql tables department87 student87
with t as (select distinct dept_id , count(*) over (partition by dept_id) cc
from student87)
select dept_name, coalesce(cc,0)
from department87 d left join t
on d.dept_id = t.dept_id
