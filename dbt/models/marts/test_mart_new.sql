{{
    config(
        materialized='table',
        alias='test_mart_new',
    )
}}
with base as (
select
du.userid,
fs2.session_sk ,
fs2.session_start_time,
fr.registration_time ,
fs2.is_paid_session,
dc.channel,
fr."__insert_timestamp"  as created_at,
extract(epoch from
	(registration_time - session_start_time))/ 3600 as diff_in_hours
from
	dwh.fact_registrations fr
left join dwh.dim_user du on du.user_sk  = fr.user_sk 
left join dwh.fact_sessions fs2 on fs2.user_sk =fr.user_sk 
left join dwh.dim_channel dc on dc.channel_sk =fs2.channel_sk 
where 1=1
	and fs2.session_start_time <= fr.registration_time 
	--and fr."__insert_timestamp" >= (select max(fr2."__insert_timestamp") from dwh.fact_registrations fr2)
	),
paid_search as (
select 'PAID_SEARCH' as channel_name,channel,userid,registration_time,min(diff_in_hours) as diff_in_hours,created_at
from base where 1=1
and diff_in_hours<=3
and channel='PAID SEARCH'
group by channel_name,channel,userid,registration_time,created_at
),
--select * from paid_search where userid=3074457348177337743;
--select base.*,p.* from base left join paid_search p 
--on p.userid = base.userid
--where
-- p.userid is null;
--and
--base.userid=3074457348177337743;
--antijoin
paid_impression as (
select 'PAID_IMPRESSION' as channel_name,b.userid,b.registration_time,min(b.diff_in_hours) as diff_in_hours,b.created_at
from base b left join paid_search p on p.userid = b.userid
where b.channel='IMPRESSION' and b.diff_in_hours<=1
and p.userid is null
group by channel_name,b.userid,b.registration_time,b.created_at
) --50417;
,
--select count(1) from paid_impression;
--paid_impression as (
--select 'PAID_IMPRESSION' as channel_name,userid,registration_time,min(diff_in_hours) as diff_in_hours,created_at
--from base b 
--where channel='IMPRESSION' --and diff_in_hours<=1
--and b.userid not in (select userid from paid_search)
--group by channel_name,userid,registration_time,created_at
--)
--organic_search as (
--select 'ORGANIC_SEARCH' as channel_name,userid,registration_time,min(diff_in_hours) as diff_in_hours,created_at
--from base b where channel='ORGANIC SEARCH' --and diff_in_hours<=12
--and b.userid not in (select userid from paid_search union all select userid from paid_impression)
--group by channel_name,userid,registration_time,created_at
--)
--select count(1) from organic_search; --75868
-- anti join
organic_search as (
select 'ORGANIC_SEARCH' as channel_name,b.userid,b.registration_time,min(b.diff_in_hours) as diff_in_hours,b.created_at
from base b left join paid_search p on p.userid = b.userid
left join paid_impression pim on pim.userid = b.userid
where b.channel='ORGANIC SEARCH' and b.diff_in_hours<=12
and p.userid is null
and pim.userid is null
group by b.userid,b.registration_time,b.created_at
)
--select count(1) from organic_search;
--,
--direct as (
--select 'DIRECT' as channel_name,userid,registration_time,min(diff_in_hours) as diff_in_hours,created_at
--from base b where channel='DIRECT' and b.userid not in ( select userid from paid_search 
--union all select userid from paid_impression 
--union all select userid from organic_search
--)
--group by channel_name,userid,registration_time,created_at
--)
--select count(1) from direct; -- 23434
,
direct as (
select 'DIRECT' as channel_name,b.userid,b.registration_time,min(b.diff_in_hours) as diff_in_hours,b.created_at
from base b left join paid_search p on p.userid = b.userid
left join paid_impression pim on pim.userid = b.userid
left join organic_search os on os.userid = b.userid
where b.channel='DIRECT'
and p.userid is null
and pim.userid is null
and os.userid is null
group by b.userid,b.registration_time,b.created_at
)
--select count(1) from direct; --23434
,
others as (
select 'OTHERS' as channel_name,b.userid,b.registration_time,min(b.diff_in_hours) as diff_in_hours,b.created_at
from base b left join paid_search p on p.userid = b.userid
left join paid_impression pim on pim.userid = b.userid
left join organic_search os on os.userid = b.userid
left join direct d on d.userid = b.userid
where 1=1
and p.userid is null
and pim.userid is null
and os.userid is null
and d.userid is null
and b.channel not in ('DIRECT','ORGANIC SEARCH', 'PAID SEARCH', 'IMPRESSION')
group by b.userid,b.registration_time,b.created_at
)
--select * from others where userid=3074457346349148105;
,
--select count(1) from others; -- 345989
--
--others as (
--select 'OTHERS' as channel_name,userid,registration_time,min(diff_in_hours) as diff_in_hours,created_at
--from base b where channel not in ('DIRECT','ORGANIC SEARCH', 'PAID SEARCH', 'IMPRESSION') 
--and b.userid not in ( select userid from paid_search 
--union all select userid from paid_impression 
--union all select userid from organic_search
--union all select userid from direct
--)
--group by channel_name,userid,registration_time,created_at
--),
--select distinct channel from others;
main as (
select channel_name,userid,registration_time, diff_in_hours,created_at from direct
union all
select channel_name,userid,registration_time, diff_in_hours,created_at from organic_search
union all
select channel_name,userid,registration_time, diff_in_hours,created_at from paid_search
union all
select channel_name,userid,registration_time, diff_in_hours,created_at from paid_impression
union all
select channel_name,userid,registration_time, diff_in_hours,created_at from others
)
select * from main