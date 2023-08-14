--Average Salary.sql tables salary, employee

with t1 as (
select left(pay_date,7) as pay_month, avg(amount) over(partition by month(pay_date),department_id) as dep_avg,
avg(amount) over(partition by month(pay_date))as comp_avg,department_id
from salary s join employee e on s.employee_id = e.employee_id)
select distinct pay_month, department_id, 
case
	when dep_avg>comp_avg then 'higher'
	when dep_avg =comp_avg then 'same'
	else 'lower'
	end as comparison
from t1
order by pay_month desc

--Cumulative Salary.sql tables employee1

with t1 as (select * , max (month) over ( partition by id) as recent
from Employee1)
select id, month , sum(salary) over(partition by id order by month) as salaryy
from t1
where Month < recent
order by id, month desc

--Department top three salaries.sql tables employee2 , department1

select a.department,a.employee,a.salary
	from (select d.name as department, e.name as employee,e.salary as salary ,
DENSE_RANK() over( partition by d.name order by salary desc) as rk
from employee2 e join Department1 d 
on e.DepartmentId = d.Id) as a
where rk <4

--Find median given frequency of numbers.sql tables numbers

	WITH T1 as (
select *,
sum(frequency) over(order by number) as cum_sum, (sum(frequency) over())/2 as middle
from numbers)

select avg(number) as median
from t1
where middle between (cum_sum - Frequency) and cum_sum

--Find the quiet students in the exam.sql tables student exam
with cte as(
	select student_id from (
select * , max(score) over(partition by exam_id) as max,
min(score) over(partition by exam_id ) as min
from exam) as a
where score = a.max or score = a.min)
select distinct s.student_id ,student_name from student s join exam e
on s.student_id =e.student_id
where s.student_id != all( select student_id from cte)
order by student_id

--Game Play Analysis 5.sql tables activity

WITH next AS (
    SELECT *,
           MIN(event_date) OVER (PARTITION BY player_id) AS install_date,
           LEAD(event_date, 1) OVER (PARTITION BY player_id ORDER BY event_date) AS nxt
    FROM Activity
)
SELECT
    install_date,
    COUNT(DISTINCT player_id) AS installs,
    round(SUM(CASE WHEN nxt = DATEADD(day, 1, event_date) THEN 1 ELSE 0 END)*1.0 / COUNT(DISTINCT player_id),2)  AS Day1_retention
FROM next
GROUP BY install_date;

--Human traffic of stadium.sql tables staduim
WITH consecutive_rows AS (
    SELECT *,id- ROW_NUMBER() OVER (ORDER BY id) AS group_num
    FROM stadium
	where people>100
)
SELECT id, visit_date, people
FROM consecutive_rows c
left join(
select group_num,count(*) as total
from consecutive_rows
group by group_num) as s
on s.group_num = c.group_num
where s.total>= 3

--Market Analysis 2.sql tables orders2 items2 users2

	with fa as (
select u.user_id,
case when rs.item_brand = u.favorite_brand  then 'yes'
else 'no'
end as item_fav_brand
from  users2 u left join
(select o.item_id,seller_id, item_brand , rank() over(partition by seller_id order by order_date) as rk
from Orders2 o join Items2 i 
on o.item_id = i.item_id) as rs
on rs.seller_id = u.user_id
where rs.rk = 2
)
select u.user_id, coalesce(item_fav_brand,'no') as '2nd_item_fav_brand'
from Users2 as u left join fa 
on fa.user_id = u.user_id


--Median Employee Salary.sql tables employee7

select distinct id,company,salary
from(
select * , row_number() over(partition by company order by salary) as rk,
count(*) over(partition by company) as cte
from employee7) as s
where rk between cte/2 and cte/2+1

--Number of transactions per visit.sql tables transactions , visits
	WITH  t1 AS(SELECT a.visit_date,
	                           COALESCE(num_visits,0) as num_visits,
	                           COALESCE(num_trans,0) as num_trans
	                    FROM ((
	                          SELECT visit_date, user_id, COUNT(*) as num_visits
	                          FROM visits
	                          GROUP BY user_id,visit_date) AS a
	                         LEFT JOIN
	                          ( SELECT transaction_date,
	                                 user_id,
	                                 count(*) as num_trans
	                            FROM transactions
								group by transaction_date,user_id) AS b
	                         ON a.visit_date = b.transaction_date and a.user_id = b.user_id) ),

              t2 AS (SELECT MAX(num_trans) as trans
                        FROM t1
                      UNION ALL
                      SELECT trans-1 
                        FROM t2
                      WHERE trans >= 1)
SELECT t2.trans as transactions_count, 
       COALESCE(visits_count,0) as visits_count
  FROM t2 LEFT JOIN (
                    SELECT num_trans as transactions_count, COALESCE(COUNT(*),0) as visits_count
                    FROM t1 
                    GROUP BY num_trans) AS a
ON a.transactions_count = t2.trans
ORDER BY t2.trans

--Report contiguous dates.sql tables succeeded , failed

select 'successed' as period_state, min(success_date) as start_date, max(success_date) as end_date
from ( select *, ROW_NUMBER() over(order by Success_date) as rk from Succeeded 
where success_date between '2019-01-01' and '2019-12-31') t
group by day(success_date) - rk
union
select 'failed' as period_state, min(fail_date) as start_date, max(fail_date) as end_date
from ( select *, ROW_NUMBER() over(order by fail_date) as rk from failed 
where fail_date between '2019-01-01' and '2019-12-31') t
group by day(fail_date) - rk
order by 2 

--Sales by day of the week.sql
SELECT
    i.item_category AS Category,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 2 THEN o.quantity ELSE 0 END) AS Monday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 3 THEN o.quantity ELSE 0 END) AS Tuesday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 4 THEN o.quantity ELSE 0 END) AS Wednesday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 5 THEN o.quantity ELSE 0 END) AS Thursday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 6 THEN o.quantity ELSE 0 END) AS Friday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 7 THEN o.quantity ELSE 0 END) AS Saturday,
    SUM(CASE WHEN DATEPART(WEEKDAY, o.order_date) = 1 THEN o.quantity ELSE 0 END) AS Sunday
FROM Orders o
JOIN Items i ON o.item_id = i.item_id
GROUP BY i.item_category
ORDER BY i.item_category;

--Students report by geography.sql tables student105

SELECT [America], [Asia], [Europe]
FROM (
    SELECT name, continent , ROW_NUMBER() over(partition by continent order by name) rk
    FROM student105
) AS SourceTable
PIVOT (
    min(name)
    FOR continent IN ([America], [Asia], [Europe])
) AS PivotTable;

--Total sales amount by year.sql Tables product114 sales114 

select *
from (

        select s.product_id, product_name, '2018' as report_year, 
               case when year(period_end)>= 2019 then average_daily_sales * (datediff(day,period_start,'2018-12-31')+1)
                    end as total_amount
        from sales114 s left join product114 p on s.product_id = p.product_id
        where year(period_start) = 2018
        union
        select s.product_id, product_name,'2019' as report_year, 
               case when year(period_start) = 2018 and year(period_end) = 2019 then average_daily_sales * datediff(day,'2019-01-01',period_end)
                    when year(period_start) = 2018 and year(period_end) = 2020 then average_daily_sales * 365
                    when year(period_start) = 2019 and year(period_end) = 2019 then average_daily_sales * (datediff(day,period_start,period_end)+1)
                    when year(period_start) = 2019 and year(period_end) = 2020 then average_daily_sales *(datediff(day,period_start,'2019-12-31')+1)
                    end as total_amount
        from sales114 s left join product114 p on s.product_id = p.product_id
        where year(period_start) < 2020 and year(period_end) > 2018
        union
        select s.product_id, product_name,'2020' as report_year, 
               case when year(period_start) < 2020 then average_daily_sales * day(period_end)
                    when year(period_start) = 2020 then average_daily_sales * (day(period_end) - day(period_start) + 1)
                    end as total_amount
        from sales114 s left join product114 p on s.product_id = p.product_id
        where year(period_end) = 2020
) a
order by product_id, report_year

--Tournament Winners.sql tables players matches2
with t as (select 
	first_player,sum(first_score) total
from
(select 
	first_player,first_score
from Matches2
union all
select 
	second_player,second_score
from Matches2)a
group by first_player),
t2 as(
select *,coalesce(total,0) tt
from Players p left join t
on p.player_id =t.first_player)
select group_id,player_id from (select *, row_number() over(partition by group_id order by tt desc,first_player) rk from t2) K
where rk =1 

--Trips and Users.sql tables users98 trips98
with t as (select Request_at,count(*) cc
from trips98
where Driver_Id in ( select Users_Id from users98 where Banned != 'Yes')
and Client_Id in ( select users_id from users98 where Banned != 'Yes')
and request_at between '2013-10-01' and '2013-10-03'
group by Request_at),
t2 as (select Request_at,count(*) tt
from trips98
where Driver_Id in ( select Users_Id from users98 where Banned != 'Yes')
and Client_Id in ( select users_id from users98 where Banned != 'Yes')
and request_at between '2013-10-01' and '2013-10-03'
and status != 'Completed'
group by Request_at)
select t.Request_at, coalesce(round(tt*1.0 /cc,2),0) rate
from t left join t2 
on t.Request_at = t2.Request_at


--User purchase platform.sql tables spending

select distinct q.spend_date,q.plat_form,ISNULL(amount,0) total_amount, ISNULL(g,0) total_users
from ( 
select a.spend_date, a.USER_ID,  a.amount, cc, (case when cc =2 then 'both' else platform end ) as platform, g from (
select spend_date, USER_ID, sum(amount) amount, count(user_id) as cc  , count(distinct user_id) as g
from Spending
group by user_id, spend_date) a
join Spending s
on a.user_id =s.user_id and a.spend_date =s.spend_date) H
right join  (SELECT DISTINCT spend_date, 'desktop' AS plat_form
  FROM spending
  UNION
  SELECT DISTINCT spend_date, 'mobile' AS plat_form
  FROM spending
  UNION
  SELECT DISTINCT spend_date, 'both' AS plat_form
  FROM spending) q
  ON q.spend_date = H.spend_date AND q.plat_form = h.platform
  order by q.spend_date,q.plat_form desc



