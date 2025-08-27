with bundle_dates AS (-- Get a list of dates and bundles
  SELECT service_date, format_datetime(service_date, 'EEEE') day_of_week, bundle, pick_year, pick_name, sched_type, manual
  FROM mtadatalake.core.dim_bus_gtfs_bundle_dates
  where bundle in ('2024Jan_Prod_r01_b03_Predate_Shuttles_v2_i1_scheduled', '2024April_Prod_r01_b07_Predate_02_Shuttles_2_SCHEDULED')
  --  where pick_year in (2024) 
)
, gtfs_calendar_fixed_dates as (
  -- Correct invalid dates like February 31st
  select service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, bundle
    , start_date/10000 start_yr, start_date/100 % 100 start_mnth
    -- Take out dates like Feb 31 or Apr 31
    , case 
      when start_date/100 % 100 = 2 and start_date % 100 > 28 then 28 
      when (start_date/10000) % 4 = 0 and start_date % 100 > 29 then 29 -- if it's a leap year, 2/29
      when start_date/100 % 100 in (9, 4, 6, 11) and start_date % 100 > 30 then 30
      else start_date % 100
    end as start_day
    , end_date/10000 end_yr, end_date/100 % 100 end_mnth
    , case
      when end_date/100 % 100 = 2 and end_date % 100 > 28 then 28
      when (end_date/10000) % 4 = 0 and end_date % 100 > 29 then 29 -- if it's a leap year, 2/29
      when end_date/100 % 100 in (9, 4, 6, 11) and end_date % 100 > 30 then 30
      else end_date % 100
    end as end_day
  from mtadatalake.core.fact_gtfs_calendar
  where bundle in (select distinct bundle from bundle_dates)
)
, calendar as (
  -- convert start_date to date object
  select service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, bundle
    , DATE(cast(start_yr AS VARCHAR) || '-' || CAST(start_mnth AS VARCHAR) || '-' || CAST(start_day AS VARCHAR)) as start_date
    , DATE(CAST(end_yr AS VARCHAR) || '-' || CAST(end_mnth AS VARCHAR) || '-' || CAST(end_day AS VARCHAR)) AS end_date
  from gtfs_calendar_fixed_dates
)
, exceptions as (
  SELECT service_id, "date", exception_type, bundle
    , DATE(cast(("date" / 10000) as varchar) || '-' || cast((("date" / 100) % 100) as varchar) || '-' || cast(("date" % 100) as varchar)) AS service_date
    , format_datetime(DATE(cast(("date" / 10000) as varchar) || '-' || cast((("date" / 100) % 100) as varchar) || '-' || cast(("date" % 100) as varchar)), 'EEEE') as day_of_week
  FROM mtadatalake.core.fact_gtfs_calendar_dates
  where bundle in (select distinct bundle from bundle_dates)
)
, base_schedule as (
  -- for each day, assemble a row for each service id serving it
  SELECT bd.service_date, bd.day_of_week, c.service_id, c.bundle, e.exception_type
  FROM bundle_dates bd
  inner join calendar c
    ON bd.bundle = c.bundle 
    and bd.service_date BETWEEN c.start_date AND c.end_date
    AND (
      (bd.day_of_week = 'Monday' AND c.monday = 1) OR
      (bd.day_of_week = 'Tuesday' AND c.tuesday = 1) OR
      (bd.day_of_week = 'Wednesday' AND c.wednesday = 1) OR
      (bd.day_of_week = 'Thursday' AND c.thursday = 1) OR
      (bd.day_of_week = 'Friday' AND c.friday = 1) OR
      (bd.day_of_week = 'Saturday' AND c.saturday = 1) OR
      (bd.day_of_week = 'Sunday' AND c.sunday = 1)
      )
  -- Join in dates and service_ids where service was removed (exception_type 2)
  left join (select * from exceptions where exception_type = 2) e
    on c.bundle = e.bundle
    and bd.service_date = e.service_date
    and c.service_id = e.service_id
)
, modified_schedules as (
  -- Join in dates and service_ids where service was added (exception_type 1)
  select service_date, day_of_week, service_id, bundle, exception_type
  from base_schedule 
  where exception_type is null or exception_type = 1
  union all
  select service_date, day_of_week, service_id, bundle, exception_type
  from exceptions 
  where exception_type = 1
)
, daily_schedules as (
  -- Designate schedule daytype
  select service_date, day_of_week, service_id, bundle
    , CASE 
        WHEN day_of_week(service_date) BETWEEN 1 AND 5 THEN 'Weekday'
        WHEN day_of_week(service_date) = 6 THEN 'Saturday'
        WHEN day_of_week(service_date) = 7 THEN 'Sunday'
        ELSE null
      end as sched_daytype
  from modified_schedules
)
, schedules_per_day AS (
    -- Aggregate service_ids for each day to form a "schedule"
    SELECT 
        bundle, service_date, sched_daytype
        , array_join(array_agg(CAST(service_id AS VARCHAR) ORDER BY service_id), ',') AS schedule
    FROM daily_schedules
    GROUP BY bundle, service_date, sched_daytype
)
, schedule_variations AS (
  -- Count the occurrences of each unique schedule
  SELECT bundle, service_date, schedule, sched_daytype
    , COUNT(service_date) over (partition by bundle, schedule, sched_daytype) AS sched_var_frequency
    -- Make a name for each unique schedule, e.g. Weekday-1, Saturday-3
    ,  sched_daytype || '-' || CAST(DENSE_RANK() OVER (PARTITION BY bundle, sched_daytype ORDER BY schedule) AS VARCHAR) AS schedule_variation
  FROM schedules_per_day
)
, schedule_variations_ranked as ( -- this CTE returns one row per day, with the schedule variation and rank
  select bundle, service_date, schedule, sched_daytype, schedule_variation, sched_var_frequency
    , row_number() over (partition by bundle, sched_daytype order by sched_var_frequency desc, service_date) ranking_by_freq
  from schedule_variations
)
, most_representative_day as ( -- this CTE will give a weekday, saturday, and sunday date for each bundle
  -- When ranking_by_freq = 1, that day is the best representation of a weekday, saturday, or sunday schedule
    select bundle, service_date, sched_daytype as day_of_week
    from schedule_variations_ranked
    where ranking_by_freq = 1
)
-- decide between schedule_variations_ranked, most_representative_day, or sched_with_service_id
select * from most_representative_day