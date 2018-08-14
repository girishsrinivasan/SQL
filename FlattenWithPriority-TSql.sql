;With RawData as
(
	select	1 as UserID,
			cast( '20100101 09:00' as datetime) as StartTime,
			cast( '20100101 23:00' as datetime) as EndTime,
			1 as Priority

	union all

	select 1 , '20100101 13:00' , '20100101 20:00', 2

	union all

	select 1 , '20100102 13:00' , '20100102 20:00', 4

	union all

	select 2 , '20100101 13:00' , '20100112 20:00', 2

),
EventTime as (
	-- Split the raw data into two rows. 
	select S.UserID, S.EventTime, S.StartStop
	from RawData
	cross apply (
			Values (RawData.UserID, RawData.StartTime, 1),	(RawData.UserID,RawData.EndTime, -1) 
	) S (UserID,EventTime,StartStop)
),
BaseSegmentCandidates as (
	select	UserID,
			EventTime as StartTime,
			lead(EventTIme)over (partition by UserID order by EventTime) EndTime
	from EventTime
),
BaseSegment as (
	select UserID, StartTime, EndTime
	from BaseSegmentCandidates
	where EndTime is not null  and StartTime <> EndTime

)
select B.*, max(A.Priority) as Priority
from BaseSegment B
-- How can we avoid this join which is sure to slow things down to a crawl?
inner join RawData A on A.UserID = B.UserID and A.StartTime < B.EndTime and A.EndTime > B.StartTime 
group by  B.UserID, B.StartTime, B.EndTime
order by B.UserID, B.StartTime