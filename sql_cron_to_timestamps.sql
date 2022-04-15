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
  from dim_numbers
  where n between 1 and 5 
)

-- unique passed cron codes
, crons as (
  select '5-29/2,31-59/4 */3 4/5 * TUE-WED,1-3' as cron
) 
-- years of timestamps to return
, years_forward as (
  select 
    date_part('year', current_date) + n - 1 as year
    , year::text as year_text
  from dim_numbers
  where n between 1 and 2
)
, crons_day_match_mode as (
  select
    cron
    -- only checking first position is defacto standard per https://crontab.guru/cron-bug.html
    , left(split_part(crons.cron, ' ', 3), 1) = '*' as uses_day_of_month_wildcard
    , left(split_part(crons.cron, ' ', 5), 1) = '*' as uses_day_of_week_wildcard
    , iff(not uses_day_of_month_wildcard and not uses_day_of_week_wildcard, 'union', 'intersect') as day_match_mode
  from crons
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
      -- replace asterisk with equivalent selector (per cron_part)
    , replace(split_part(crons.cron, ' ', space_number.n), '*', cron_part_defaults.star_range) as cron_part_entry_raw
    , case 
        when space_number.n = 5
        then 
          replace(replace(replace(replace(
            replace(replace(replace(replace(
              upper(cron_part_entry_raw), '7', '6'), 'SUN', '0'), 'MON', '1'), 'TUE', '2')
            , 'WED', '3'), 'THU', '4'), 'FRI', '5'), 'SAT', '6')
        when space_number.n = 4
        then 
          replace(replace(replace(replace(replace(replace(
            replace(replace(replace(replace(replace(replace(
              upper(cron_part_entry_raw), 'JAN', '1'), 'FEB', '2'), 'MAR', '3'), 'APR', '4'), 'MAY', '5'), 'JUN', '6')
            , 'JUL', '7'), 'AUG', '8'), 'SEP', '9'), 'OCT', '10'), 'NOV', '11'), 'DEC', '12')
        else cron_part_entry_raw
      end as cron_part_entry
    , comma_numbers.n as cron_part_entry_comma_index
    , split_part(cron_part_entry, ',', cron_part_entry_comma_index) as cron_part_comma_subentry
    , split_part(cron_part_comma_subentry, '/', 1) as cron_part_comma_subentry_range
    , coalesce(nullif(split_part(cron_part_comma_subentry, '/', 2), '')::int, 1) as cron_part_comma_subentry_step_value
    , split_part(cron_part_comma_subentry_range, '-', 1)::int as cron_part_comma_subentry_range_start
    , coalesce(
        nullif(split_part(cron_part_comma_subentry_range, '-', 2), '')::int
        -- blank fill with range max if step, otherwise, range start again
        , case 
            when regexp_count(cron_part_comma_subentry, '/') = 1
            then split_part(cron_part_defaults.star_range, '-', 2)::int
            else cron_part_comma_subentry_range_start
          end
      ) as cron_part_comma_subentry_range_end
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
)
, cron_timestamps as (
  select 
    crons.cron
    , years.year
    , cron_part_minute.value_text as minute_value_text
    , cron_part_hour.value_text as hour_value_text
    , cron_part_month.value_text as month_value_text
    , month_days.value_text as day_of_month_value_text
    , to_timestamp_ntz(
        concat(
          years.year_text, '-', month_value_text, '-', day_of_month_value_text
          , ' '
          , hour_value_text, ':', minute_value_text
        )
      ) as cron_trigger_at

  from crons_day_match_mode as crons
  inner join years_forward as years
    on true
  inner join cron_part_matched_values as cron_part_minute
    on crons.cron = cron_part_minute.cron
    and cron_part_minute.cron_part = 'minute'
  inner join cron_part_matched_values as cron_part_hour
    on crons.cron = cron_part_hour.cron
    and cron_part_hour.cron_part = 'hour'
  inner join cron_part_matched_values as cron_part_month
    on crons.cron = cron_part_month.cron
    and cron_part_month.cron_part = 'month'
  -- fan to all days of month
  inner join cron_part_values as month_days
    on month_days.cron_part = 'day_of_month'
    -- filter days beyond month end (Feb 30ths)
    and month_days.value <= date_part('day', last_day(to_date(concat(years.year_text, '-', cron_part_month.value_text, '-', '01'))))
  -- left matched day_of_month
  left join cron_part_matched_values as cron_part_day_of_month
    on crons.cron = cron_part_day_of_month.cron
    and cron_part_month.value = cron_part_day_of_month.value
    and cron_part_day_of_month.cron_part = 'day_of_month'
  -- left matched day_of_week
  left join cron_part_matched_values as cron_part_day_of_week
    on crons.cron = cron_part_day_of_week.cron
    and mod(date_part('dayofweek', to_date(concat(years.year_text, '-', cron_part_month.value_text, '-', month_days.value_text))), 7) = cron_part_day_of_week.value
    and cron_part_day_of_week.cron_part = 'day_of_week'

  where 
    (crons.day_match_mode = 'union' 
     and (cron_part_day_of_month.value is not null 
          or cron_part_day_of_week.value is not null))
    or 
    (crons.day_match_mode = 'intersect' 
     and cron_part_day_of_month.value is not null
     and cron_part_day_of_week.value is not null)

   order by cron_trigger_at asc
)

select *
from cron_timestamps;
