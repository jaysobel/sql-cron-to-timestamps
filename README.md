### Snowflake SQL Cron to Timestamps

Takes one or more rows containing cron schedule expressions and outputs all their matching timestamps within specified years.

Written in Snowflake SQL, which provides handy functions like `last_day('2022-02-01'::date) -- 28`. I could have made better use of [split_to_table](https://docs.snowflake.com/en/sql-reference/functions/split_to_table.html), but I like joining to numbers, and it's also closer to being cross-compatible.

I have not verified the accuracy of the output, but it is quite quick!

Based on the rules described in [crontab guru](https://crontab.guru/#5-29/2_4_4/5_*_TUE-WED,1-3).

[Brief explanation](https://stackoverflow.com/questions/34357126/why-crontab-uses-or-when-both-day-of-month-and-day-of-week-specified) on day-of-month (position 3) and day-of-week (position 5) being combined with logical OR when they are both restricted (not wildcarded, and specifically only checking for a wildcard in the first character; eg `*/2` vs `1,*/2`). The [Crontab spec](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html) does not clarify this case, but crontab guru has a good write up on the so-called [crontab bug](https://crontab.guru/cron-bug.html) which relates to only checking the first character for a wildcard.
