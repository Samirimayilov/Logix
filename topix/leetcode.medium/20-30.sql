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
--Monthly Transaction 2.sql
with trans_tbl as(
select left( trans_date, 8) as month, country,
count(*) as approved_count,
sum(amount) as approved_anount
from transactions2
where state= 'approved'
group by left( trans_date, 8),
country),
charge_tbl as(
select left( c.trans_date, 8) as month, country,
count(*) as chargeback_count,
sum(amount) as chargeback_amount
from Chargebacks c join Transactions2 t
on c.trans_id = t.id
group by left( c.trans_date, 8),
country),
jj as (
select MONTH , country from trans_tbl
union 
select month, country from charge_tbl)
select jj.month, jj.country,
ISNULL(approved_count,0),
ISNULL(approved_anount,0),
ISNULL(chargeback_count,0),
ISNULL(chargeback_amount,0)
from jj left join trans_tbl t
on jj.month = t.month  and jj.country = t.country
left join charge_tbl c
on jj.month = c.month  and jj.country = c.country
--Monthly Transactions 1.sql
with t as (
select LEFT(trans_date,7) month, country, count(distinct id) trans_count, sum(amount) as trans_total_amount 
from Transactions3
group by LEFT(trans_date,7) , country),
t1 as (
select LEFT(trans_date,7) month, country, count(distinct id) approved_count, sum(amount) as approved_total_count 
from Transactions3
where state = 'approved'
group by LEFT(trans_date,7) , country)
select t.month,t.country,trans_count,approved_count,trans_total_amount,approved_total_count
from t join t1
on t.month = t1.month and t1.country = t.country
order by month,country desc

--Movie Rating.sql
select * from(
select top 1 u.name from Movie_Rating5 m join Users5 u 
on m.user_id = u.user_id
group by u.user_id,u.name
order by name, COUNT(*)) A
union 
select * from (
select top 1 o.title as name from Movie_Rating5 m join Movies5 o
on m.movie_id = o.movie_id
where left(m.created_at,7) = '2020-02'
group by title
order by avg(rating) ) a

--Page Recommnedations.sql
with t as (
select distinct user1_id user1, user2_id friend from (
select user1_id,user2_id
from Friendship
union all
select user2_id, user1_id
from Friendship) a)
select distinct page_id 
from Likes l join t
on l.user_id = t.user1
where user_id in (select friend from t where user1 = 1)
and page_id not in ( select page_id from Likes where user_id = 1)

--Product Price at a given date.sql
with t as 
(select product_id,max(new_price) maxp 
from Products
where change_date <= '2019-08-16'
group by product_id)
select distinct p.product_id,ISNULL(t.maxp,10) as price from Products p left join t
on p.product_id = t.product_id


--Product Sales Analysis 3.sql
select s.product_id , year , quantity , price from (
select *,RANK() over(partition by product_id order by year) rk from sales9) s join Product9 p 
on s.product_id = p.product_id
where rk = 1
