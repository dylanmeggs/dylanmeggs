{{
    config(
        sort='calendar_date'
    )
}}

SELECT reportdateid                           AS id,
       calendardate :: DATE                   AS calendar_date,

       DATE_PART('Week', calendar_date)       AS calendar_week,
       {{ str_std("TO_CHAR(calendar_date, 'Day')") }}          AS calendar_day_of_week_name,
       LEFT(calendar_day_of_week_name, 3)     AS calendar_day_of_week_name_abbr,
       DATE_PART('Month', calendar_date)      AS calendar_month,
       {{ str_std("TO_CHAR(calendar_date, 'Month')") }}          AS calendar_month_name,
       {{ str_std("TO_CHAR(calendar_date, 'Mon')") }}            AS calendar_month_name_abbr,
       DATE_PART('Quarter', calendar_date)    AS calendar_quarter,
       DATE_PART_YEAR(calendar_date)          AS calendar_year,
       dayofyear                              AS calendar_day_of_year,

       fiscalweek                             AS reporting_week,
       {{ str_std("weekdayname") }}                            AS reporting_day_of_week_name,
       LEFT(reporting_day_of_week_name, 3)                     AS reporting_day_of_week_name_abbr,
       fiscalweekbegin :: DATE                AS reporting_week_start_date,
       fiscalweekend :: DATE                  AS reporting_week_end_date,
       month                                  AS reporting_month,
       {{ str_std("monthname") }}                              AS reporting_month_name,
       LEFT(reporting_month_name, 3)                           AS reporting_month_name_abbr,
       fiscalmonthstart :: DATE               AS reporting_month_start_date,
       fiscalmonthend :: DATE                 AS reporting_month_end_date,
       REPLACE(UPPER(TRIM(fiscalquarter)), 'Q', '') :: INT AS reporting_quarter,
       fiscalquarterstart :: DATE             AS reporting_quarter_start_date,
       fiscalquarterend :: DATE               AS reporting_quarter_end_date,
       fiscalyear                             AS reporting_year,
       fiscalyearstart :: DATE                AS reporting_year_start_date,
       fiscalyearend :: DATE                  AS reporting_year_end_date,

       calendar_date = (CURRENT_TIMESTAMP AT TIME ZONE 'america/los_angeles') :: DATE AS is_current_date,
       DATEDIFF(WEEK, reporting_week_start_date,
                (DATE_TRUNC('Week', (CURRENT_TIMESTAMP AT TIME ZONE 'america/los_angeles')) -
                 INTERVAL '1 Day') :: DATE)                                           AS current_reporting_week_offset
FROM {{ source('org_bie', 'stage_reporting_calendar') }} AS d