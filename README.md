### Snowflake SQL Cron to Timestamps

Takes one or more rows containing cron schedule expressions and outputs all their matching timestamps in years to come.

Assumes a table `dim_numbers` with a single column `n` containing integers between 1 and 60, or higher.

Written in Snowflake SQL, which provides handy functions like `last_day('2022-02-01'::date) -- 28`. I could have made better use of [split_to_table](https://docs.snowflake.com/en/sql-reference/functions/split_to_table.html), but I like joining to numbers.

Not tested, but the example cron expression outputs 10k rows in a couple seconds.

Based on the rules described in [crontab guru](https://crontab.guru/#5-29/2_4_4/5_*_TUE-WED,1-3).

[Brief explanation](https://stackoverflow.com/questions/34357126/why-crontab-uses-or-when-both-day-of-month-and-day-of-week-specified) on day-of-month (position 3) and day-of-week (position 5) being combined with logical OR when they are both restricted.

It remains unclear to me how a step or comma separated wildcard should be interpretted this; ex: `(* * 1,*/2, *, */3,5)`

[Crontab spec](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html)


>In the POSIX locale, the user or application shall ensure that a crontab entry is a text file consisting of lines of six fields each. The fields shall be separated by \<blank\> characters. The first five fields shall be integer patterns that specify the following:
>
> Minute [0,59]
>
> Hour [0,23]
>
> Day of the month [1,31]
>
> Month of the year [1,12]
>
> Day of the week ([0,6] with 0=Sunday)
>
> Each of these patterns can be either an \<asterisk\> (meaning all valid values), an element, or a list of elements separated by \<comma\> characters. An element shall be either a number or two numbers separated by a \<hyphen-minus\> (meaning an inclusive range). The specification of days can be made by two fields (day of the month and day of the week). If month, day of month, and day of week are all \<asterisk\> characters, every day shall be matched. If either the month or day of month is specified as an element or list, but the day of week is an \<asterisk\>, the month and day of month fields shall specify the days that match. If both month and day of month are specified as an \<asterisk\>, but day of week is an element or list, then only the specified days of the week match. Finally, if either the month or day of month is specified as an element or list, and the day of week is also specified as an element or list, then any day matching either the month and day of month, or the day of week, shall be matched.
