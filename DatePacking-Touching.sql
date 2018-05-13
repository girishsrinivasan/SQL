;With BaseData as
(
	select 1 as UserID,  cast( '20100101 20:00' as datetime) as StartTime, cast( '20100101 21:00' as datetime) as EndTime
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

),Adjacent as 
(
	select 
		UserID, 
		StartTime, 
		EndTime,
		-- sum(TouchesPrevious) is a running total. Since TouchesPrevious = 1 if it does not touch we end up with the 
		-- same running totals for rows wich touch each other when ordered by start time (since they have 0 and do not change the running total)
		-- This assume that there are no overlaps other than for touching in the base data
		sum(TouchesPrevious) over (partition by UserID order by StartTime) grp  
	from (
		select 
		UserID, 
		StartTime, 
		EndTime,
		-- TouchesPrevious is 1 if this interval touches the previous interval
		case when StartTime = lag(EndTime) over (partition by UserID order by StartTime) then 0 else 1 end  TouchesPrevious
	from BaseData
	) a
)
select 
   UserID, StartTime =min(StartTime), EndTime=max(EndTime)
from Adjacent
group by UserID , grp
order by UserID,StartTime