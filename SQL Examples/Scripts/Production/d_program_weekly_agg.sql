/*
Concept:
Each program contains a variety of lowest granularity fact/dimension tables.
Some of these can have metrics of their own.
From there, we can create tables which aggregate those metrics across different groupings and time periods.
Our broader DE team also uses what we call a data layer table. This consolidates every possible aggregation grouping, time period, etc.
This is not ideal in all scenarios, but it allows us to create a central repository / source of truth for metrics across teams.
I can hard code certain values in 1-3 tables and have those cascade down to 15+ other locations.
*/


drop table if exists region_details;
create temp table region_details AS
SELECT 
          pst.location_super_region     AS super_region
        , pst.location_sub_super_region AS sub_super_region
        , pst.region                    AS region
        , pst.area                      AS sub_region
        , pst.location_id               AS station
FROM schema_na.d_location_table pst;


-- Description of temp table / metric associated with it here
drop table if exists trailing_4wk_compl;
create temp table trailing_4wk_compl AS

WITH WeekWindow AS (SELECT DISTINCT reporting_week_start_date,
                                    DATEADD(WEEK, -5, reporting_week_start_date)     AS window_start_date, -- 5 Sundays prior
                                    DATEADD(WEEK, -2, reporting_week_start_date) + 6 AS window_end_date    -- Saturday of prior week
                    FROM team_prod_schema.dim_reporting_calendar
                    WHERE reporting_week_start_date >= DATEADD(WEEK, -51, CURRENT_DATE)
                      AND reporting_week_start_date <= CURRENT_DATE),

     ClosedCases AS (SELECT w.reporting_week_start_date as reporting_date
                          , orc.delivery_type           as business
                          , orc.country
                          , orc.continent
                          , orc.dsp_shortcode
                          , orc.station_code
                          , COUNT(*)                    AS closed_trainings
                     FROM WeekWindow w
                              LEFT JOIN team_prod_schema.d_team_escalation orc
                                        ON orc.date_created BETWEEN w.window_start_date AND w.window_end_date
                                            AND
                                           orc.date_closed BETWEEN w.window_start_date AND w.reporting_week_start_date
                     WHERE orc.behavior_bucket IN ('SAMPLE A','SAMPLE B')
                       AND orc.resolution_code IN ('SAMPLE A', 'SAMPLE B', 'SAMPLE C')
                       AND orc.status = 'RESOLVED'
                       AND orc.continent = 'NA'
                     GROUP BY w.reporting_week_start_date
                            , orc.delivery_type
                            , orc.country
                            , orc.continent
                            , orc.dsp_shortcode
                            , orc.station_code),

     TotalCases AS (SELECT w.reporting_week_start_date as reporting_date
                         , orc.delivery_type           as business
                         , orc.country
                         , orc.continent
                         , orc.dsp_shortcode
                         , orc.station_code
                         , COUNT(*)                    AS total_trainings
                    FROM WeekWindow w
                             LEFT JOIN team_prod_schema.d_team_escalation orc
                                       ON orc.date_created BETWEEN w.window_start_date AND w.window_end_date
                    WHERE orc.behavior_bucket IN ('SAMPLE A','SAMPLE B')
                      AND orc.resolution_code IN ('SAMPLE A', 'SAMPLE B', 'SAMPLE C')
                      AND orc.continent = 'NA'
                    GROUP BY w.reporting_week_start_date
                           , orc.delivery_type
                           , orc.country
                           , orc.continent
                           , orc.dsp_shortcode
                           , orc.station_code)


SELECT tc.reporting_date
     , tc.business
     , tc.country
     , tc.continent
     , tc.dsp_shortcode
     , tc.station_code
     , r.super_region
     , r.sub_super_region
     , r.region
     , r.sub_region
     , COALESCE(cc.closed_trainings, 0) AS closed_trainings
     , tc.total_trainings               AS total_trainings
     , CASE
           WHEN tc.total_trainings = 0 THEN 0 ::numeric(6, 5)
    --ELSE round((1.0 * COALESCE(cc.closed_cases, 0) / tc.total_cases) * 100.0, 2)::numeric(5,2)
           ELSE round((1.0 * COALESCE(cc.closed_trainings, 0) / tc.total_trainings), 5)::numeric(6, 5)
    END                                 AS trailing_4wk_compl
FROM TotalCases tc
         LEFT JOIN ClosedCases cc
                   ON tc.reporting_date = cc.reporting_date
                       AND tc.country = cc.country
                       AND tc.continent = cc.continent
                       AND tc.dsp_shortcode = cc.dsp_shortcode
                       AND tc.station_code = cc.station_code
         LEFT JOIN region_details r
ON tc.station_code = r.station
;

drop table if exists lifetime_volume;
create temp table lifetime_volume AS
    SELECT
    orc.delivery_type                              as business
  , orc.country
  , orc.continent
  , orc.dsp_shortcode
  , orc.station_code
  , COUNT(*) AS lifetime_assigned
  , count(case when is_closed then 1 end)          as lifetime_closed
  , count(case when is_closed = 0 then 1 end)      as lifetime_open
  FROM team_prod_schema.d_team_escalation orc
  WHERE orc.behavior_bucket IN ('SAMPLE A','SAMPLE B')
    AND orc.resolution_code IN ('SAMPLE A', 'SAMPLE B', 'SAMPLE C')
    AND orc.continent = 'NA'

  GROUP BY
          orc.delivery_type
        , orc.country
        , orc.continent
        , orc.dsp_shortcode
        , orc.station_code
;


drop table if exists avg_days;
create temp table avg_days AS
SELECT orc.week_created_start_date as reporting_date
     , orc.delivery_type           as business
     , orc.country                 as country
     , orc.continent               as continent
     , orc.dsp_shortcode
     , orc.station_code
     , r.super_region
     , r.sub_super_region
     , r.region
     , r.sub_region

     , avg
    (case
         when orc.resolution_code in ('SAMPLE A', 'SAMPLE B', 'SAMPLE C')
             AND orc.status = 'RESOLVED'
             AND orc.behavior_bucket = 'SAMPLE'
             then DATEDIFF(day, orc.date_appeal_window_started, orc.date_closed)
         else null end)            as avg_days_to_completion
     , avg(dtr.days_to_repeat)     as avg_days_to_repeat

from team_prod_schema.d_team_escalation orc
         LEFT JOIN
     (Select *
      from (select lag(date_created) over (partition by transporter_id, behavior order by date_created) as prior
                 , DATEDIFF(day, prior, date_created)                                                   as days_to_repeat
                 , *
            from team_prod_schema.d_team_escalation
            where resolution_code != 'EXAMPLE')
      where resolution_code = 'EXAMPLE') dtr
     ON orc.case_number = dtr.case_number
         LEFT JOIN region_details r
on orc.station_code = r.station

GROUP BY
    orc.week_created_start_date
        , orc.delivery_type
        , orc.country
        , orc.continent
        , orc.dsp_shortcode
        , orc.station_code
        , r.super_region
        , r.sub_super_region
        , r.region
        , r.sub_region
;



drop table if exists final_table;
create temp table final_table AS
SELECT ad.reporting_date
     , ad.business
     , ad.country
     , ad.continent
     , ad.dsp_shortcode
     , ad.station_code
     , ad.super_region
     , ad.sub_super_region
     , ad.region
     , ad.sub_region
     , t.closed_trainings
     , t.total_trainings
     , t.trailing_4wk_compl as orcas_trailing_4wk_completion_rate
     , ad.avg_days_to_completion
     , ad.avg_days_to_repeat
     , lv.lifetime_assigned
     , lv.lifetime_closed
     , lv.lifetime_open

FROM lifetime_volume lv
LEFT JOIN avg_days ad
    ON ad.country = lv.country
    AND ad.continent = lv.continent
    AND ad.dsp_shortcode = lv.dsp_shortcode
    AND ad.station_code = lv.station_code
LEFT JOIN trailing_4wk_compl t
    ON ad.reporting_date = t.reporting_date
    AND ad.business = t.business
    AND ad.country = t.country
    AND ad.dsp_shortcode = t.dsp_shortcode
    AND ad.station_code = t.station_code
;


SELECT reporting_date
     , business
     , country
     , continent
     , dsp_shortcode
     , station_code
     , super_region
     , sub_super_region
     , region
     , sub_region
     , closed_trainings
     , total_trainings
     , orcas_trailing_4wk_completion_rate
     , avg_days_to_completion
     , avg_days_to_repeat
     , lifetime_assigned
     , lifetime_closed
     , lifetime_open
FROM final_table
;