/*+ ETLM {
     depend:{
         add:[
             {name:"teamname_prod.d_program_escalation"},
             {name:"teamname_prod.product_org_weekly_events_na",cutoff:{hours:3}},
             {name:"teamname_prod.d_xyz_program_30d_compl"}
         ]
     }
  }*/

/*
Description: 
This query output feeds into x table which contains all reporting metrics for y program.

Change Log:
2023-09-29
Adding a,b,c metrics
 
2023-06-22
Swapping values in provider_type and business_type columns.

2023-06-08 
Removing insert_date and update_date. These will be handled via load job profile. Also updating metric_updated_till_date.
 */

---------------------------------------------------------------------
--regions
---------------------------------------------------------------------

create temp table region_{TEMPORARY_TABLE_SEQUENCE} as (
select
  case
    when orc.continent = 'NA'
      then 1
    when orc.continent = 'EU'
      then 2
    when orc.continent = 'FE'
      then 3
    when orc.continent = 'IN'
      then 4
    when orc.continent = 'SA'
      then 5
  end as region_id
, case_number
from teamname_prod.d_program_escalation orc
where orc.continent != 'EU'
);

---------------------------------------------------------------------
-- raw aggregations
---------------------------------------------------------------------
create temp table aggregations_{TEMPORARY_TABLE_SEQUENCE} as (
select distinct
  orc.continent                                               as region
  ,orc.country                                                as country
  ,r.region_id                                                as region_id
  ,orc.program                                                as provider_type
  ,lower(orc.delivery_type)::varchar(100)                     as business_type
  ,ca.reporting_year||lpad(ca.reporting_week, 2, '0')::varchar as week
  ,ca.reporting_week_end_date                                 as week_report_period
  ,ca.reporting_year||lpad(ca.reporting_month, 2, '0')::varchar as month
  ,mo.month_report_period::date                               as month_report_period
  ,ca.reporting_year||lpad(ca.reporting_quarter, 2, '0')::varchar as quarter
  ,qu.quarter_report_period::date                             as quarter_report_period
  ,ca.reporting_year::varchar                                 as year
  ,TO_DATE(ca.reporting_year || '-01-01', 'YYYY-MM-DD')       as year_report_period
  ,count
    (case when orc.is_appeal = true
      then orc.case_number
    else null end)                                            as appealed
  ,count
    (case when orc.is_appeal = true and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as low_sev_appealed
  ,count
    (case when orc.is_appeal = true and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as high_sev_appealed
  ,count
    (case when orc.is_appeal_approved = true
      then orc.case_number
    else null end)                                            as appeal_approved
  ,count
    (case when orc.is_appeal_approved = true and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as low_sev_appeal_approved
  ,count
    (case when orc.is_appeal_approved = true and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as high_sev_appeal_approved
  , count
    (case when orc.resolution_code <> 'DUPLICATE'
            AND orc.not_an_infraction_reason <> 'DUPLICATE'
        then orc.case_number
    else null end)                                            as total_cases_all
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as valid_low_sev
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as valid_high_sev
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM LOW SEV'
    and orc.resolution_code = 'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
      then orc.case_number
    else null end)                                            as sec_ocr
  ,count
    (case when orc.is_valid = true
    and orc.resolution_code = 'PROGRAM 3RD OCCURRENCE (OFFBOARDING)'
    --and orc.behavior_bucket = 'PROGRAM HIGH SEV'
    --and orc.behavior = '3+ OCCURRENCES OF SPEEDING 10+ MPH FOR 5 SECONDS'
        then orc.case_number
    else null end)                                            as offboards_3rd_o
  ,avg(dtr.days_to_repeat)                                    as days_to_repeat
  ,avg
    (case when orc.resolution_code in
        ('PROGRAM 1ST OCCURRENCE'
         ,'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
         ,'APPEAL REJECTED')
        AND orc.status = 'RESOLVED'
      then DATEDIFF(day, orc.date_appeal_window_started, orc.date_closed)
    else null end)                                            as days_to_completion
    -- Need to confirm avg vs sum on the 3 below
  ,avg(product_denominator)                                 as trips
  ,avg(comp.closed)                                           as compl_closed
  ,avg(comp.total)                                            as compl_total

from teamname_prod.d_program_escalation orc
join region_{TEMPORARY_TABLE_SEQUENCE} r
  on orc.case_number = r.case_number

join teamname_prod.dim_reporting_calendar ca
 on orc.date_created = ca.calendar_date

join -- table used for quarter reporting period
  (select distinct
    reporting_year||'-'||
    min(lpad(reporting_month,2,'0'))
      over (partition by reporting_year,reporting_quarter)
    ||'-01' as quarter_report_period
    , reporting_quarter
    , reporting_year
   from teamname_prod.dim_reporting_calendar
   where
     calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
   ) qu
    on ca.reporting_year = qu.reporting_year
    and ca.reporting_quarter = qu.reporting_quarter

join -- table used for month reporting period
  (select distinct
    reporting_year||'-'||lpad(reporting_month,2,'0')||'-01' as month_report_period
    , reporting_month
    , reporting_year
   from teamname_prod.dim_reporting_calendar
   where
     calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
   ) mo
    on ca.reporting_year = mo.reporting_year
    and ca.reporting_month = mo.reporting_month

left join -- pulls the telematics trip counts
  (select
     event_week
     ,program
     ,country
     ,region
     ,sum(product_denominator) as product_denominator
   from teamname_prod.product_org_weekly_events_na
   group by 1,2,3,4) nw
  on ca.reporting_week_end_date = nw.event_week+6
  and orc.delivery_type = nw.program
  and orc.country = nw.country
  and orc.continent = nw.region

left join teamname_prod.d_xyz_program_30d_compl comp -- pulls the training completion
  on ca.reporting_week_end_date = comp.date-1
  -- Rolling 4wk completion rate has 2 week lag
  -- This means date value of 2023-06-18 aggregates values between 2023-05-14 (Sun - Start of Wk) and 2023-06-10 (Sat - End of Wk)
  and orc.delivery_type = comp.delivery_type
  and orc.country = comp.country
  and orc.continent = comp.region

left join -- days to re-offend
  (select *
   from
      (select
        lag(date_created) over (partition by transporter_id, behavior order by date_created)  as prior
        , DATEDIFF(day, prior, date_created) as days_to_repeat
        , *
       from teamname_prod.d_program_escalation
       where resolution_code != 'PROGRAM DUPLICATE')
   where resolution_code = 'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
  ) dtr
  on orc.case_number = dtr.case_number

where
  ca.calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
  and orc.continent != 'EU'

group by grouping sets
  (
-- region
  (orc.continent, week_report_period, week, orc.delivery_type, orc.program, r.region_id),
  (orc.continent, month_report_period, month, orc.delivery_type, orc.program, r.region_id),
  (orc.continent, quarter_report_period, quarter, orc.delivery_type, orc.program, r.region_id),
  (orc.continent, year_report_period, year, orc.delivery_type, orc.program, r.region_id),
-- country
  (orc.country,  week_report_period, week, orc.delivery_type, orc.program, r.region_id),
  (orc.country,  month_report_period, month, orc.delivery_type, orc.program, r.region_id),
  (orc.country,  quarter_report_period, quarter, orc.delivery_type, orc.program, r.region_id),
  (orc.country,  year_report_period, year, orc.delivery_type, orc.program, r.region_id)
  )

union

select distinct
  orc.continent                                               as region
  ,orc.country                                                as country
  ,r.region_id                                                as region_id
  ,orc.program                                                as provider_type
  ,'overall'                                                  as business_type
  ,ca.reporting_year||lpad(ca.reporting_week, 2, '0')::varchar as week
  ,ca.reporting_week_end_date                                 as week_report_period
  ,ca.reporting_year||lpad(ca.reporting_month, 2, '0')::varchar as month
  ,mo.month_report_period::date                               as month_report_period
  ,ca.reporting_year||lpad(ca.reporting_quarter, 2, '0')::varchar as quarter
  ,qu.quarter_report_period::date                             as quarter_report_period
  ,ca.reporting_year::varchar                                 as year
  ,TO_DATE(ca.reporting_year || '-01-01', 'YYYY-MM-DD')       as year_report_period
  ,count
    (case when orc.is_appeal = true
      then orc.case_number
    else null end)                                            as appealed
  ,count
    (case when orc.is_appeal = true and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as low_sev_appealed
  ,count
    (case when orc.is_appeal = true and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as high_sev_appealed
  ,count
    (case when orc.is_appeal_approved = true
      then orc.case_number
    else null end)                                            as appeal_approved
  ,count
    (case when orc.is_appeal_approved = true and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as low_sev_appeal_approved
  ,count
    (case when orc.is_appeal_approved = true and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as high_sev_appeal_approved
  , count
    (case when orc.resolution_code <> 'DUPLICATE'
            AND orc.not_an_infraction_reason <> 'DUPLICATE'
        then orc.case_number
    else null end)                                            as total_cases_all
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM LOW SEV'
      then orc.case_number
    else null end)                                            as valid_low_sev
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM HIGH SEV'
      then orc.case_number
    else null end)                                            as valid_high_sev
  ,count
    (case when orc.is_valid = true
    and orc.behavior_bucket = 'PROGRAM LOW SEV'
    and orc.resolution_code = 'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
      then orc.case_number
    else null end)                                            as sec_ocr
  ,count
    (case when orc.is_valid = true
    and orc.resolution_code = 'PROGRAM 3RD OCCURRENCE (OFFBOARDING)'
    --and orc.behavior_bucket = 'PROGRAM HIGH SEV'
    --and orc.behavior = '3+ OCCURRENCES OF SPEEDING 10+ MPH FOR 5 SECONDS'
        then orc.case_number
    else null end)                                            as offboards_3rd_o
  ,avg(dtr.days_to_repeat)                                    as days_to_repeat
  ,avg
    (case when orc.resolution_code in
        ('PROGRAM 1ST OCCURRENCE'
         ,'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
         ,'APPEAL REJECTED')
        AND orc.status = 'RESOLVED'
      then DATEDIFF(day, orc.date_appeal_window_started, orc.date_closed)
    else null end)                                            as days_to_completion
  ,avg(nw.product_denominator)                              as trips
  ,avg(comp.closed)                                           as compl_closed
  ,avg(comp.total)                                            as compl_total

from teamname_prod.d_program_escalation orc

join region_{TEMPORARY_TABLE_SEQUENCE} r
  on orc.case_number = r.case_number

join teamname_prod.dim_reporting_calendar ca
  on orc.date_created = ca.calendar_date

join -- table used for quarter reporting period
  (select distinct
    reporting_year||'-'||
    min(lpad(reporting_month,2,'0'))
      over (partition by reporting_year,reporting_quarter)
    ||'-01' as quarter_report_period
    , reporting_quarter
    , reporting_year
   from teamname_prod.dim_reporting_calendar
   where
     calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
   ) qu
    on ca.reporting_year = qu.reporting_year
    and ca.reporting_quarter = qu.reporting_quarter

join -- table used for month reporting period
  (select distinct
    reporting_year||'-'||lpad(reporting_month,2,'0')||'-01' as month_report_period
    , reporting_month
    , reporting_year
   from teamname_prod.dim_reporting_calendar
   where
     calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
   ) mo
    on ca.reporting_year = mo.reporting_year
    and ca.reporting_month = mo.reporting_month

left join -- pulls the telematics trip counts
  (select
     event_week
     ,country
     ,region
     ,sum(product_denominator) as product_denominator
   from teamname_prod.product_org_weekly_events_na
   group by 1,2,3) nw
  on ca.reporting_week_end_date = nw.event_week+6
  and orc.country = nw.country
  and orc.continent = nw.region

left join -- pulls the training completion
  (select
    date
    ,country
    ,region
    ,sum(total) as total
    ,sum(closed) as closed
   from teamname_prod.d_xyz_program_30d_compl comp
   group by 1,2,3) comp
  on ca.reporting_week_end_date = comp.date-1
  and orc.country = comp.country
  and orc.continent = comp.region

left join -- days to re-offend
  (select *
   from
      (select
        lag(date_created) over (partition by transporter_id, behavior order by date_created)  as prior
        , DATEDIFF(day, prior, date_created) as days_to_repeat
        , *
       from teamname_prod.d_program_escalation
       where resolution_code != 'PROGRAM DUPLICATE')
   where resolution_code = 'PROGRAM 2ND OCCURRENCE (SUSPENSION)'
  ) dtr
  on orc.case_number = dtr.case_number

where
  ca.calendar_date
    between (select min(date_created) from teamname_prod.d_program_escalation)
            and current_date
  and orc.continent != 'EU'

group by grouping sets
  (
-- region
  (orc.continent,  week_report_period, week, orc.program, r.region_id),
  (orc.continent,  month_report_period, month, orc.program, r.region_id),
  (orc.continent,  quarter_report_period, quarter, orc.program, r.region_id),
  (orc.continent,  year_report_period, year, orc.program, r.region_id),
-- country
  (orc.country,  week_report_period, week, orc.program, r.region_id),
  (orc.country,  month_report_period, month, orc.program, r.region_id),
  (orc.country,  quarter_report_period, quarter, orc.program, r.region_id),
  (orc.country,  year_report_period, year, orc.program, r.region_id)
  )
)
;



---------------------------------------------------------------------
-- program_completion_rate
---------------------------------------------------------------------

create temporary table final_{TEMPORARY_TABLE_SEQUENCE} as (
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_completion_rate'        as metric_name
  ,compl_closed                   as metric_num
  ,compl_total                    as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null
  and metric_value is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_completion_rate'        as metric_name
  ,compl_closed                   as metric_num
  ,compl_total                    as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null
  and metric_value is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_completion_rate'        as metric_name
  ,compl_closed                   as metric_num
  ,compl_total                    as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
  ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null
  and metric_value is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_completion_rate'        as metric_name
  ,compl_closed                   as metric_num
  ,compl_total                    as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null
  and metric_value is not null

---------------------------------------------------------------------
-- program_low_sev_cases_per_x_y
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_per_x_y'        as metric_name
  ,valid_low_sev                  as metric_num
  ,trips                          as metric_den
  ,case
    when metric_den = 0
      then 0
    else ((metric_num*1.0/metric_den*1.0)*100)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null
  and metric_value is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_per_x_y'        as metric_name
  ,valid_low_sev                  as metric_num
  ,trips                          as metric_den
  ,case
    when metric_den = 0
      then 0
    else ((metric_num*1.0/metric_den*1.0)*100)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null
  and metric_value is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_per_x_y'        as metric_name
  ,valid_low_sev                  as metric_num
  ,trips                          as metric_den
  ,case
    when metric_den = 0
      then 0
    else ((metric_num*1.0/metric_den*1.0)*100)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null
  and metric_value is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_per_x_y'        as metric_name
  ,valid_low_sev                  as metric_num
  ,trips                          as metric_den
  ,case
    when metric_den = 0
      then 0
    else ((metric_num*1.0/metric_den*1.0)*100)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null
  and metric_value is not null

---------------------------------------------------------------------
-- program_low_sev_appeal_rate
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeal_rate'    as metric_name
  ,low_sev_appealed               as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeal_rate'    as metric_name
  ,low_sev_appealed               as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeal_rate'    as metric_name
  ,low_sev_appealed               as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeal_rate'    as metric_name
  ,low_sev_appealed               as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null



---------------------------------------------------------------------
-- program_appeal_rate
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_rate'    as metric_name
  ,appealed                       as metric_num
  ,total_cases_all                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_rate'    as metric_name
  ,appealed                       as metric_num
  ,total_cases_all                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_rate'    as metric_name
  ,appealed                       as metric_num
  ,total_cases_all                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_rate'    as metric_name
  ,appealed                       as metric_num
  ,total_cases_all                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null


---------------------------------------------------------------------
-- program_2nd_occurrence_rate
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_2nd_occurrence_rate'    as metric_name -- AKA Recidivism rate
  ,sec_ocr                        as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_2nd_occurrence_rate'    as metric_name
  ,sec_ocr                        as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_2nd_occurrence_rate'    as metric_name
  ,sec_ocr                        as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_2nd_occurrence_rate'    as metric_name
  ,sec_ocr                        as metric_num
  ,valid_low_sev                  as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null

---------------------------------------------------------------------
-- program_low_sev_cases_raw
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_raw'      as metric_name
  ,valid_low_sev                  as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_raw'      as metric_name
  ,valid_low_sev                  as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_raw'      as metric_name
  ,valid_low_sev                  as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_cases_raw'      as metric_name
  ,valid_low_sev                  as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null

---------------------------------------------------------------------
-- program_high_sev_cases_raw
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_high_sev_cases_raw'     as metric_name
  ,valid_high_sev                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_high_sev_cases_raw'     as metric_name
  ,valid_high_sev                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_high_sev_cases_raw'     as metric_name
  ,valid_high_sev                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_high_sev_cases_raw'     as metric_name
  ,valid_high_sev                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null


---------------------------------------------------------------------
-- program_appeals_approved
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeals_approved'       as metric_name
  ,appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeals_approved'       as metric_name
  ,appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeals_approved'       as metric_name
  ,appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeals_approved'       as metric_name
  ,appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null



---------------------------------------------------------------------
-- program_offboards_third_occurrence_raw
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                                         as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                                         as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                                         as dim_value
  ,'program_offboards_third_occurrence_raw'       as metric_name
  ,offboards_3rd_o                              as metric_num
  ,1                                            as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                                         as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                                   as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_offboards_third_occurrence_raw'       as metric_name
  ,offboards_3rd_o                              as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  , 'program_offboards_third_occurrence_raw'       as metric_name
  , offboards_3rd_o                              as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  , 'program_offboards_third_occurrence_raw'          as metric_name
  , offboards_3rd_o                                 as metric_num
  ,1                                                as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null





---------------------------------------------------------------------
-- program_low_sev_appeals_approved
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeals_approved'       as metric_name
  ,low_sev_appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeals_approved'       as metric_name
  ,low_sev_appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeals_approved'       as metric_name
  ,low_sev_appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_low_sev_appeals_approved'       as metric_name
  ,low_sev_appeal_approved                as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null






---------------------------------------------------------------------
-- program_appeal_success_rate
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_success_rate'     as metric_name
  ,appeal_approved                 as metric_num
  ,appealed                        as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_success_rate'     as metric_name
  ,appeal_approved                 as metric_num
  ,appealed                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_success_rate'     as metric_name
  ,appeal_approved                 as metric_num
  ,appealed                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_appeal_success_rate'     as metric_name
  ,appeal_approved                 as metric_num
  ,appealed                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null

---------------------------------------------------------------------
-- program_avg_days_to_reoffend
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_reoffend'     as metric_name
  ,days_to_repeat                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null
  and metric_value is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_reoffend'     as metric_name
  ,days_to_repeat                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null
  and metric_value is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_reoffend'     as metric_name
  ,days_to_repeat                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null
  and metric_value is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_reoffend'     as metric_name
  ,days_to_repeat                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null
  and metric_value is not null

---------------------------------------------------------------------
-- program_avg_days_to_completion
---------------------------------------------------------------------
union
-- weekly
select
  region_id
  ,provider_type
  ,business_type
  ,'weekly'                       as aggregation
  ,week                           as report_period
  ,week_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_completion'     as metric_name
  ,days_to_completion                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  week is not null
  and metric_value is not null

union
-- monthly
select
  region_id
  ,provider_type
  ,business_type
  ,'monthly'                      as aggregation
  ,month                          as report_period
  ,month_report_period            as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_completion'     as metric_name
  ,days_to_completion                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  month is not null
  and metric_value is not null

union
-- quarterly
select
  region_id
  ,provider_type
  ,business_type
  ,'quarterly'                    as aggregation
  ,quarter                        as report_period
  ,quarter_report_period          as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_completion'     as metric_name
  ,days_to_completion                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  quarter is not null
  and metric_value is not null

union
-- yearly
select
  region_id
  ,provider_type
  ,business_type
  ,'yearly'                       as aggregation
  ,year                           as report_period
  ,year_report_period             as report_date
  ,case
    when country is not null
      then 'CN'
    when region is not null
      then 'RG'
    end                           as dim_code
  ,case
    when country is not null
      then 'country'
    when region is not null
      then 'region'
    end                           as dim_type
  ,case
    when country is not null
      then country
    when region is not null
      then region
    end                           as dim_value
  ,'program_avg_days_to_completion'     as metric_name
  ,days_to_completion                 as metric_num
  ,1                              as metric_den
  ,case
    when metric_den = 0
      then 0
    else (metric_num*1.0/metric_den*1.0)
    end                           as metric_value
   , TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD') - 1                   as metric_updated_till_date
   ,'dmeggs'                     as updated_by

from aggregations_{TEMPORARY_TABLE_SEQUENCE}

where
  year is not null
    and metric_value is not null
);

SELECT
    region_id
  , provider_type
  , business_type
  , aggregation
  , report_period
  , report_date
  , dim_code
  , dim_type
  , dim_value
  , metric_name
  , metric_num
  , metric_den
  , metric_value
  , metric_updated_till_date
  , updated_by
FROM final_{TEMPORARY_TABLE_SEQUENCE}