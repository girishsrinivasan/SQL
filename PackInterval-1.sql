-- Ref: Overlapping ranges with priority
-- https://stewashton.wordpress.com/2014/03/22/overlapping-ranges-with-priority/

with intervals as (
    -- a values
    select cast( '20100101 08:00' as timestamp) as Start, cast( '20100101 13:00' as timestamp) as End, 'a1' as Value
    union all
    select '20100101 15:00' as Start, '20100101 17:00' as End, 'a2' as Value
    -- b values
    union all
    select cast( '20100101 10:00' as timestamp) as Start, cast( '20100101 11:00' as timestamp) as End, 'b1' as Value
    union all
    select '20100101 16:00' as Start, '20100101 18:00' as End, 'b2' as Value
 ),
 
 -- Actual processing starts here 

-- Split into event times (i.e each interval is now two rows - one with start time and one with end time)
events as (
     select EventTime,Dense_Rank() Over(order by EventTime) as EventTimeRank
     from intervals A
     join lateral (
         select EventTime 
         from ( Values(A.Start), (A.End) ) s(EventTime)
    ) split on true
),

-- Create the base ranges for the merged intervals
baseRanges as (
    select BaseStart, BaseEnd, EventTimeRank
    from (
		select EventTime as BaseStart,EventTimeRank, LEAD(EventTime) over(order by EventTime) as BaseEnd
		from events
    ) A    
    where A.BaseEnd is not null
),

-- Create a grouping column. This works because the base ranges cover intervals for the whole thing. 
-- We create a rank for the interval based on its start time. 
-- This is sequential and covers everything. In the select below we get rid of intervals that are gaps. 
-- This causes the sequence to miss some number (the ones for the pseudo intervals that were gotten rid of through the join for overlap)
-- Then to get the group we do the standard thing of subtracting from a sequence that has no gaps. 
-- This gives us a group number which is the same for intervals that need to be packed together 
rangesWithGroup as (
    select BaseStart, BaseEnd, EventTimeRank - Row_number() over( order by EventTimeRank) as Grp
    from BaseRanges BR
    inner join intervals A on A.Start < BaseEnd and A.End > BaseStart -- Gets rid of intervals that are actually gaps (i.e those that do not intersect with an actual input interval)
    group by BaseStart, BaseEnd, EventTimeRank
    order by BaseStart,BaseEnd
)
-- do the final packing based on the group number
select min(BaseStart) as Start, max(BaseEnd) as End 
from rangesWithGroup
group by Grp
order by Start