### Snowflake SQL Cron to Timestamps

Takes one or more rows containing cron schedule expressions and outputs all their matching timestamps in years to come.

Assumes a table `dim_numbers` with a single column `n` containing integers between 1 and 60, or higher.

Written in Snowflake SQL, which provides handy functions like `last_day('2022-02-01'::date) -- 28`. I could have made better use of [split_to_table](https://docs.snowflake.com/en/sql-reference/functions/split_to_table.html), but I like joining to numbers.

Not tested, but the example cron expression outputs 10k rows in a couple seconds.

Based on the rules described in [crontab guru](https://crontab.guru/#5-29/2_4_4/5_*_TUE-WED,1-3).
