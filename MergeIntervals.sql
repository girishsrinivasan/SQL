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
     select EventTime,Value 
     from intervals A
     join lateral (
         select EventTime 
         from ( Values(A.Start), (A.End) ) s(EventTime)
    ) split on true
),

-- Create the base ranges for the merged intervals
baseRanges as (
    select BaseStart, BaseEnd
    from (
		select EventTime as BaseStart, LEAD(EventTime) over(order by EventTime) as BaseEnd
		from events
    ) A    
    where A.BaseEnd is not null
)
select BaseStart, BaseEnd, array_to_string(array_agg(A.Value), ', ')
from BaseRanges BR
inner join intervals A on A.Start < BaseEnd and A.End > BaseStart
group by BaseStart, BaseEnd
order by BaseStart
