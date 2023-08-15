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
--All people report to the given manager.sql 
