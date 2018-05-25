;With BaseData as
(
	select 1 as UserID,  cast( '20100101 20:00' as datetime) as StartTime, cast( '20100104 21:00' as datetime) as EndTime
	union all
	select 1 , '20100101 21:00' , '20100102 02:00' 
	union all 
	select 1 ,'20100102 02:00' , '20100102 03:00' 
	union all 
	select 1 , '20100102 03:00' , '20100102 05:00' 

	union all
	select 1 , '20100103 10:00' , '20100103 11:00' 

	union all
	select 2 , '20100101 20:00' , '20100103 11:00' 

),
MaxPreviousEnd as (
	select UserID, StartTime, EndTime,	max(EndTime) over (partition by UserID order by StartTime rows between unbounded preceding and 1 preceding) MaxPreviousEnd 
	from  BaseData
),
IntersectsWithPrevious as (
	select UserID, StartTime, EndTIme, case when MaxPreviousEnd >= StartTime then 0 else 1 end as TouchesPrevious 
	from  MaxPreviousEnd
),
IntervalGroups as (
select 
		UserID, 
		StartTime, 
		EndTime,
		sum(TouchesPrevious) over (partition by UserID order by StartTime rows  unbounded preceding ) grp  
		from IntersectsWithPrevious
)
select 
   UserID, StartTime =min(StartTime), EndTime=max(EndTime)
from IntervalGroups
group by UserID , grp
order by UserID,StartTime