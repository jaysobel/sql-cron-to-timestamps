with cron_part_values as (
  select 
    'minute' as cron_part
    , n - 1 as value
    , right(concat('0', value::text), 2) as value_text
  from dim_numbers
  where n between 1 and 60
  
    union all 

  select 
    'hour' as cron_part
    , n - 1 as value
    , right(concat('0', value::text), 2) as value_text
  from dim_numbers
  where n between 1 and 24
  
    union all
  
  select 
    'day_of_month' as cron_part
    , n as value
    , right(concat('0', value::text), 2) as value_text
  from dim_numbers
  where n between 1 and 31
  
    union all
  
  select 
    'month' as cron_part
    , n as value
    , right(concat('0', value::text), 2) as value_text
  from dim_numbers
  where n between 1 and 12
  
    union all
  
  select 
    'day_of_week' as cron_part
    , n as value
    , value::text as value_text
  from dim_numbers
  where n between 0 and 6
)

, cron_part_defaults as (
  select
    n as part_number
    , case n
        when 1 then 'minute'
        when 2 then 'hour'
        when 3 then 'day_of_month'
        when 4 then 'month'
        when 5 then 'day_of_week'
      end as cron_part
    , case n
         when 1 then '0-59'
         when 2 then '0-23'
         when 3 then '1-31'
         when 4 then '1-12'
         when 5 then '0-6'
      end star_range
    , case n
        when 1 then '59'
        when 2 then '23'
        when 3 then '31'
        when 4 then '12'
        when 5 then '6'
      end stepped_range_end
  from dim_numbers
  where n between 1 and 5 
)

-- unique passed cron codes
, crons as (
  select '5-29/2,31-59/4 4 4/5 * TUE-WED,1-3' as cron
) 

-- years of timestamps to return
, years_forward as (
  select 
    date_part('year', current_date) + n - 1 as year
    , year::text as year_text
  from dim_numbers
  where n between 1 and 2
)

, cron_part_comma_subentries as (
  select 
    crons.cron
    , case space_number.n
        when 1 then 'minute'
        when 2 then 'hour'
        when 3 then 'day_of_month'
        when 4 then 'month'
        when 5 then 'day_of_week'
      end as cron_part
      -- replace wildcard * with equivalent selector (per cron_part)
    , case
        when split_part(crons.cron, ' ', space_number.n) = '*'
        then cron_part_defaults.star_range
        else split_part(crons.cron, ' ', space_number.n)
      end as cron_part_entry_raw
    , case 
        when space_number.n = 5
        then replace(replace(replace(replace(replace(replace(replace(replace(upper(cron_part_entry_raw), '7', '6'), 'SUN', '0'), 'MON', '1'), 'TUE', '2'), 'WED', '3'), 'THU', '4'), 'FRI', '5'), 'SAT', 6)
        else cron_part_entry_raw
      end as cron_part_entry
    , comma_numbers.n as cron_part_entry_comma_index
    , split_part(cron_part_entry, ',', cron_part_entry_comma_index) as cron_part_comma_subentry
    , split_part(cron_part_comma_subentry, '/', 1) as cron_part_comma_subentry_range
    , coalesce(nullif(split_part(cron_part_comma_subentry, '/', 2)::int, ''), 1) as cron_part_comma_subentry_step_value
    , split_part(cron_part_comma_subentry_range, '-', 1)::int as cron_part_comma_subentry_range_start
    , case
        -- missing range end and stepped range -> default end is max of range, for part
        when split_part(cron_part_comma_subentry_range, '-', 2) = ''
         and regexp_count(cron_part_comma_subentry, '/') > 0
        then cron_part_defaults.stepped_range_end
        -- coalesce(end, start) as end, for later inclusive between
        else coalesce(
               nullif(split_part(cron_part_comma_subentry_range, '-', 2), '')::int
               , cron_part_comma_subentry_range_start
             )
      end cron_part_comma_subentry_range_end
  from crons
  inner join dim_numbers as space_number
    on space_number.n between 1 and 5
  left join cron_part_defaults
    on space_number.n = cron_part_defaults.part_number
  inner join dim_numbers as comma_numbers
    on regexp_count(split_part(crons.cron, ' ', space_number.n), ',') + 1 >= comma_numbers.n
    and comma_numbers.n between 1 and 10 -- sets max comma-separated values within each part
)

, cron_part_matched_values as (
  select
    cpcs.cron
    , cpcs.cron_part
    , cpcs.cron_part_entry
    , cpv.value
    , cpv.value_text
      -- lists comma-separated subentries that matched the value
    , listagg(cpcs.cron_part_comma_subentry, ', ') within group (order by cpcs.cron_part_entry_comma_index asc) as matching_subentries_list
  from cron_part_comma_subentries cpcs
  inner join cron_part_values cpv
    on cpcs.cron_part = cpv.cron_part
    and cpv.value between cpcs.cron_part_comma_subentry_range_start and cpcs.cron_part_comma_subentry_range_end
    -- mod step size (inclusive of start)
    and mod(cpv.value - cpcs.cron_part_comma_subentry_range_start, cpcs.cron_part_comma_subentry_step_value) = 0
  group by 1,2,3,4,5
),

cron_timestamps as (
  select 
    cron_part_minute.cron
    , years.year
    , cron_part_minute.value_text as minute_value_text
    , cron_part_hour.value_text as hour_value_text
    , cron_part_month.value_text as month_value_text
    , month_days.value_text as day_of_month_value_text
    , case
      when cron_part_day_of_month.cron_part is null
        then 'Day of Week'
        when cron_part_day_of_week.cron_part is null
        then 'Day of Month'
        else 'Both'
      end as day_of_matching_case
    , to_timestamp(
        concat(
          years.year_text, '-', month_value_text, '-', day_of_month_value_text
          , ' '
          , hour_value_text, ':', minute_value_text
        )
      ) as cron_trigger_at

  -- cross join parts
  from years_forward as years
  inner join cron_part_matched_values as cron_part_minute
    on cron_part_minute.cron_part = 'minute'
  inner join cron_part_matched_values as cron_part_hour
    on cron_part_minute.cron = cron_part_hour.cron
    and cron_part_hour.cron_part = 'hour'
  inner join cron_part_matched_values as cron_part_month
    on cron_part_minute.cron = cron_part_month.cron
    and cron_part_month.cron_part = 'month'
  -- fan to month days, spine for 'or' match of [day_of_month or day_of_week]
  inner join cron_part_values as month_days
    on month_days.cron_part = 'day_of_month'
    -- filter out days beyond month end (Feb 30ths)
    and month_days.value <= date_part('day', last_day(to_date(concat(years.year_text, '-', cron_part_month.value_text, '-', '01'))))
  left join cron_part_matched_values as cron_part_day_of_month
    on cron_part_minute.cron = cron_part_day_of_month.cron
    and month_days.value = cron_part_day_of_month.value
    and cron_part_day_of_month.cron_part = 'day_of_month'
  left join cron_part_matched_values as cron_part_day_of_week
    on cron_part_minute.cron = cron_part_day_of_week.cron
    and mod(date_part('dayofweek', to_date(concat(years.year_text, '-', cron_part_month.value_text, '-', month_days.value_text))), 7) = cron_part_day_of_week.value
    and cron_part_day_of_week.cron_part = 'day_of_week'

  -- day_of_month or day_of_week matches
  where (cron_part_day_of_month.cron_part is not null or cron_part_day_of_week.cron_part is not null)
  order by cron_trigger_at asc
)

select *
from cron_timestamps
;
