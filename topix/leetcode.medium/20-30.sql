--Get highest answer rate question.sql survey_log
select question_id
from (select top 1 question_id,  sum(case when Action = 'answer' then 1 else 0 end)*1.0/ count(*)  cc from survey_log group by question_id)a
--Get the second most recent activity.sql 
with t as (select * , min(startdate) over(partition by username) as first , 
DENSE_RANK() over(partition by username order by startdate) rk,count(*) over(partition by username) as cnt 
from UserActivity)
select username,activity,startDate,endDate from t
where rk = 2 or cnt = 1
--Highest grade for each student.sql enrollments
select distinct student_id,course_id,grade from(
select * , DENSE_RANK() over(partition by student_id order by grade desc,course_id) rk
from enrollments) a
where rk =1
--Immediate Food Delivery 2.sql  Delivery82
with t as (select *, rank() over(partition by customer_id order by order_date) as first_order 
from Delivery82)
select avg(case when datediff(day,order_date,customer_pref_delivery_date) = 0 then 1.0 else 0 end )
from t
where first_order = 1
--Investments in 2016.sql insurance
select sum(TIV_2016) TIV_2016 
from 
(select *, count(*) over (partition by TIV_2015) as c1, count(*) over (partition by LAT, LON) as c2
from insurance ) t
where c1 > 1 and c2 = 1
--Last person to fit in the elevator.sql Queue68
with t as (
select * , sum(weight) over(order by turn) sa
from Queue68)
select person_name
from t
where turn = ( select max(turn) from t where sa <= 1000)
--Managers with atleast 5 direct reports.sql employee76
select m.Name
from employee76 e join employee76 m
on e.ManagerId = m.Id
group by m.Name
having count(*) > = 5 
--Market Analysis 1.sql orders2
select user_id,join_date,cc from 
users2 u  left join (
select buyer_id,COUNT(*) cc
from orders2
where YEAR(order_date) = 2019
group by buyer_id) a
on u.user_id = a.buyer_id
--
