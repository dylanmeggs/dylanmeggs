-- Various scrubbed artifact templates. Showcase a handful of repeatable samples (out of hundreds)





create table schema_name.reporting_table_name (
    reporting_day date,
    aggregation varchar(100),
    report_period varchar(100),
    country_code varchar(10),
    abc varchar(10),
    location varchar(256),
    user_id varchar(768),
    mechanism varchar(6144),
    alert_category varchar(768),
    alert_subcategory varchar(6144),
    alert_severity varchar(6144),
    alert_severity_subtype varchar(6144),
    metric_name varchar(200),
    metric_name_explained varchar(256),
    metric_value numeric(32,18),
    metric_num numeric(32,18),
    metric_den numeric(32,18),
    --is_current_record boolean,
    insert_timestamp_pt timestamp)
    sortkey(insert_timestamp_pt)
                                           ;

--drop table --schema_name.reporting_table_name;

GRANT ALL ON schema_name.reporting_table_name TO "CDO:acme:xyz:dbuser:groupname";

GRANT ALL ON schema_name.reporting_table_name to group groupname;

GRANT SELECT ON schema_name.reporting_table_name to group "abc-access-read-only";

-- Load partition on sortkey for as-was reporting





/*+ETLM
{
  depend:{
    add:[
      {
        name:"schema_name.d_table_name"
      }
    ]
  }
}*/
/*
Change Log:
YYYY-MM-DD
A. New resolution codes expected as part of ABC Salesforce migration. SIM: https://url-for-tracking-project/12345
B. Removing [] from [].

YYYY-MM-DD - Integrating with Salesforce [] config updates

YYYY-MM-DD - Replacing manual entry column_name drop down field from []. Using xyz logic instead.

Note:
Data cleaning in upstream stage assigns uppercase to all varchar fields (See https://reference-url/12345)
*/

DROP TABLE IF EXISTS #program_staging_table;
CREATE TEMP TABLE #program_staging_table as
SELECT ded.key,
       ded.case_number,
       ded.user_id,
       ded.user_name,
       ded.user_status,
       case
        	when len(ded.location_code) > 4 and ded.program = 'ABC'
            	then 'INVALID'
    		else ded.location_code
    	  end                             			as location_code,
       ded.abc_shortcode,
       ded.abc_name,
       CASE
           WHEN ded.behavior IN ('GENERIC STATIC HARD CODING REFERENCE',
                                 'GENERIC STATIC HARD CODING REFERENCE')
               THEN 'GENERIC STATIC HARD CODING REFERENCE'
           ELSE ded.behavior
           END AS behavior,
       ded.unique_category,
       ded.unique_bucket,
       ded.status,
       ded.resolution_code,
       ded.subject,
       ded.date_created,
       ded.date_closed,
       ded.date_workflow_window_started,
       ded.date_workflow_window_ended,
       ded.dataset_day,
       ded.is_closed :: INT AS is_closed,
       ded.code_reason,
       ded.internal_annotations,
        case
        	when ded.location_code like 'A%' then 'GENERICNAME'
        	when ded.location_code like 'B%' then 'GENERICNAME'
            when ded.location_code like 'C%' then 'GENERICNAME'
        else 'GENERICNAME'
        end as delivery_type,
        ded.origin,
        ded.country,
        ded.continent,
        ded.year_created,
        ded.month_created,
        ded.week_created,
        ded.week_created_start_date,
        ded.user_cohort_week,
        ded.user_cohort_month,
        ded.user_cohort_quarter,
        ded.user_cohort_year,
        ded.user_tenure,
        ded.user_tenure_bucket,
        ded.abc_tenure,
        ded.abc_tenure_bucket,
        ded.abc_acme_tenure,
        ded.abc_acme_bucket,
        ded.date_of_incident,
	    ded.program,
	    ded.description,
        case
            when coalesce(ded.resolution_code,'') like '%APPEAL%'
                or coalesce(ded.code_reason,'') like '%APPEAL%'
                then 1
        else 0
        end                     as is_appeal,
        case
            when coalesce(ded.resolution_code,'') like '%APPEAL APPROVED%'
                or coalesce(ded.code_reason,'') like '%APPEAL APPROVED%'
                then 1
        else 0
        end                     as is_appeal_approved,
       case
            when coalesce(ded.resolution_code,'') like '%DUPLICATE%'
                or coalesce(ded.code_reason,'') like '%DUPLICATE%'
                    then 1
        else 0
        end                     as is_duplicate,
        case
            when coalesce(ded.resolution_code,'') IN
            ('ABC ACTIONED'
            ,'GENERIC ALREADY ACTIONED'
            ,'USER ACTIONED BY ABC')
                    then 1
        else 0
        end                     as abc_actioned,
        case
            when coalesce(ded.resolution_code,'') = 'ABC EXITED'
                    then 1
        else 0
        end                     as abc_exited,
        case
            when coalesce(ded.subject,'') = 'ABC USER ACTION'
                    then 1
        else 0
        end                     as is_action_b

FROM schema_name.d_table_name AS ded
WHERE ded.data_source = 'HARD CODE ME THIS'
  AND ded.case_record_type = 'GENERIC NAME'
  AND (ded.column IN ('PROGRAM', 'GENERIC NAME') OR subject = 'ABC USER ACTION')
;





-----------------------------------------------------------------------------------------------

SELECT

        stg.key,
        stg.case_number,
        stg.user_id,
        stg.user_name,
        stg.user_status,
        stg.location_code,
        stg.abc_shortcode,
        stg.abc_name,
        stg.behavior,
        stg.unique_category,
        stg.unique_bucket,
        stg.status,
        stg.resolution_code,
        stg.subject,
        stg.date_created,
        stg.date_closed,
        stg.date_appeal_window_started,
        stg.date_appeal_window_ended,
        stg.dataset_day,
        stg.is_closed,
        stg.code_reason,
        stg.internal_annotations,
        stg.delivery_type,
        stg.origin,
        stg.country,
        stg.continent,
        stg.year_created,
        stg.month_created,
        stg.week_created,
        stg.week_created_start_date,
        stg.user_cohort_week,
        stg.user_cohort_month,
        stg.user_cohort_quarter,
        stg.user_cohort_year,
        stg.user_tenure,
        stg.user_tenure_bucket,
        stg.abc_tenure,
        stg.abc_tenure_bucket,
        stg.abc_xyz_tenure,
        stg.abc_xyz_bucket,
        stg.date_of_incident,
	    stg.program,
        stg.is_appeal,
        stg.is_appeal_approved,
        stg.is_duplicate,
        stg.abc_actioned,
        stg.abc_exited,
        stg.is_action_b,
       case
       	when stg.unique_bucket = 'PROGRAM LOW SEV'
            and stg.is_appeal_approved = 0
            and stg.is_duplicate = 0
            and stg.abc_exited = 0
            and stg.abc_actioned = 0
       	    and coalesce(stg.resolution_code,'') in
            ('PROGRAM X INSTANCE'
            , 'PROGRAM Y INSTANCE (SUSPENSION)'
            , 'PROGRAM APPEAL REJECTED'
            , 'PROGRAM NO ABC RESPONSE'
            , 'PROGRAM GENERIC - ACTION'
            , 'ACTION REJECTED')
                then 1
        when stg.unique_bucket = 'PROGRAM CATEGORY B'
            and stg.is_workflow_approved = 0
            and stg.is_duplicate = 0
            and stg.abc_exited = 0
            and stg.abc_actioned = 0
            and coalesce(stg.resolution_code,'') not like 'NOT A%'
            and coalesce(stg.resolution_code,'') not in (
                'SYSTEM ERROR',
                'COACHING REQUEST',
                'OTHER - NOT IN SCOPE'
                )
                then 1
        when stg.unique_bucket = 'PROGRAM CATEGORY B'
            and stg.resolution_code is null
                then 1
        else 0 end 		                    as is_valid,
      case
            when coalesce(stg.resolution_code,'') in ('NO ABC RESPONSE',
                                                     'PROGRAM NO ABC RESPONSE')
                then 1
            else 0
        end                                 as no_abc_response,
       case
            when coalesce(stg.resolution_code,'') = 'PROGRAM Z INSTANCE (ACTION)'
	            or coalesce(stg.code_reason,'') = 'PROGRAM Z INSTANCE'
                    then 1
            else 0
        end                                 as is_third_instance,
    case
        when stg.date_created >= 'YYYY-MM-DD'
            and is_valid = 1
            and (
                coalesce(stg.resolution_code,'') = 'PROGRAM Y INSTANCE (SUSPENSION)'
                or
                 coalesce(stg.code_reason,'') = 'CATEGORY B PROGRAM SIGNAL'
                )
                then 1
        when stg.date_created < 'YYYY-MM-DD'
            and is_valid = 1
            and (
                coalesce(stg.resolution_code,'') = 'PROGRAM Y INSTANCE (SUSPENSION)'
                or
                (no_abc_response = 1 and stg.description = 'PROGRAM Y INSTANCE')
                )
                then 1
        else 0
    end                                     as is_second_instance,
       case
            when stg.date_created >= 'YYYY-MM-DD'
                and is_valid = 1
                and stg.status = 'RESOLVED'
                --and stg.user_status = ''
                and coalesce(stg.code_reason,'') in (
                                                                'CATEGORY B PROGRAM SIGNAL',
                                                                'PROGRAM Z INSTANCE'
                    )
                    then 1
            when stg.date_created < 'YYYY-05-22' --First instance of a CATEGORY B action was on this date. Previously all CATEGORY Bs resulted in {action}.
                and (stg.unique_bucket = 'PROGRAM CATEGORY B' or is_third_instance = 1)
                and is_valid = 1
                and stg.status = 'RESOLVED'
                    then 1
        else 0
        end                                 as permanent_action,
       case
           when is_valid = 1
                -- Process rolled out YYYY-MM-DD
                and stg.date_created >= 'YYYY-MM-DD'
                and coalesce(stg.code_reason,'') = 'CATEGORY B PROGRAM SIGNAL - X INSTANCE'
                    then 1

        else 0
        end                                 as permanent_actionxyz,

       case
            when stg.date_created <= 'YYYY-MM-DD'
              and stg.unique_bucket IN ('PROGRAM CATEGORY A', 'XYZ')
              and stg.resolution_code
                IN ('PROGRAM X INSTANCE', 'PROGRAM Y INSTANCE (SUSPENSION)', 'SCENARIO REJECTED')
                  then 1
            when is_valid = 1
                and permanent_action = 0
                    then 1
        else 0
        end                                 as action_assigned,
       case
            when coalesce(stg.code_reason,'') = 'PROGRAM ACTION ACCEPTED'
                    then 1
        else 0
        end                                 as action_accepted,
       case
            when stg.date_created <= 'YYYY-MM-DD'
              and action_assigned = 1
              and status = 'RESOLVED'
                      then 1
            when action_assigned = 1
              and (action_accepted = 1 or
                    permanent_action = 1
        -- Permanent inactivation will occur after 14 day action completion window on CATEGORY B X INSTANCEs
        -- permanent_actionxyz = 1
        )
                      then 1
        else 0
        end                                 as action_closed,

    case
        when is_valid = 1
            and permanent_actionxyz = 0
            and permanent_action = 0
            and (is_second_instance = 1 or stg.description = 'PROGRAM X INSTANCE SUSPENSION')
                then 1
    else 0
    end                                     as temporary_actionxyz
FROM #program_staging_table stg
;

------------------------------------------------------------------------------------------------------------------



/*+ETLM
{
  depend:{
    add:[
      {
        name:"schema_name.d_table_name"
      },
      {
        name:"schema_name.fact_table_name_weekly_region",
        cutoff:{
          hours:3
        }
      }
    ]
  }
}*/
/*
SIM Intake: https://url-for-tracking-project/12345

Change Log:
YYYY-MM-DD
Shifting date window to avoid null reporting week on Saturday run date

YYYY-MM-DD
New logic tied to [] and updated base case table

YYYY-MM-DD
Adjusting from ratio (divide by 1) to additive logic for all additive metrics. This ensures enhaced UDL generates proper values at higher levels of granularity.

YYYY-MM-DD
Adjusting action rate logic for improved join mapping accuracy.
*/

-----------------------------------------------------------------
/*Final Rollup Table */
-----------------------------------------------------------------

drop table if exists final_rollup;
create temp table final_rollup
(
    region_id                integer       not null encode az64,
    program_type             varchar(10)   not null,
    aggregation              varchar(100)  not null,
    report_period            varchar(100)  not null,
    report_date              date          not null encode az64,
    dim_type                 varchar(100)  not null,
    dim_value                varchar(1000) not null,
    metric_name              varchar(200)  not null,
    metric_value             numeric(32,18) encode az64,
    metric_num               numeric encode az64,
    metric_den               numeric encode az64,
    metric_updated_till_date date encode az64,
    insert_date              date encode az64,
    update_date              date encode az64,
    updated_by               varchar(256),
    constraint a_abc_actuals_ww_pkey
        primary key (region_id, program_type, aggregation, report_period, report_date, dim_type, dim_value, metric_name)
);

-----date dimension: returns reporting period
drop table if exists Week_End_Date;
create temporary table Week_End_Date sortkey (week) diststyle even
as
   (select min(rc.calendar_year)                                                as reporting_year
        , to_date(min(rc.calendar_year), 'YYYY-MM-DD')                          as year
        , min(rc.calendar_quarter)::varchar                                     as reporting_quarter
        , date_trunc('quarter', rc.reporting_week_start_date)::date             as quarter
        , min(rc.calendar_quarter)                                              as calendar_quarter
        , min(rc.calendar_month)                                                as month_num
        , to_date(min(rc.calendar_year)||right('0' + month_num, 2), 'YYYYMM')   as month
        , rc.reporting_week_start_date                                          as week_start_date
        , rc.reporting_week_end_date                                            as week_end_date
        , (min(rc.reporting_year) * 100 + rc.reporting_week)                    as week
    from schema_name.d_calendar_table_name rc
    --Earliest records start in November YYYY
    where week_start_date >= 'YYYY-MM-DD'
    and calendar_date <= (DATE_TRUNC('week', (TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')+2)) - INTERVAL '1 WEEK')+5
        GROUP BY reporting_week, reporting_week_start_date, reporting_week_end_date);


-- Description which explains the trailing window design
drop table if exists action_data;
create temp table action_data AS

WITH WeekWindow AS (SELECT DISTINCT reporting_week_start_date, -- Sunday
                                    DATEADD(WEEK, -6, reporting_week_start_date)     as window_start_date, -- 6 Sundays prior
                                    DATEADD(WEEK, -3, reporting_week_start_date) + 6 as window_end_date,   -- 2 Saturdays Prior
                                    reporting_week_start_date - 1                    as closure_end_date   -- Previous day; 1 Saturday Prior
                    FROM schema_name.d_calendar_table_name
                    WHERE reporting_week_start_date >= 'YYYY-MM-DD'
                    AND reporting_week_start_date <= (DATE_TRUNC('week', (TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')+2)) - INTERVAL '1 WEEK')+5),

     ClosedCases AS (SELECT w.reporting_week_start_date             as reporting_date
                          , lower(stg.delivery_type)::varchar(100)  as program_type
                          , stg.country
                          , stg.continent
                          , stg.abc_shortcode
                          , stg.location_code
                          , COUNT(*)                                as closed_actions
                     FROM WeekWindow w
                              LEFT JOIN schema_name.d_table_name stg
                                        -- Date Workflow Window Started = Date Action Assigned
                                          ON stg.date_workflow_window_started BETWEEN w.window_start_date AND w.window_end_date
                                            AND
                                        -- Date Closed = Date ABC Submitted Action Completion Certificate, usually +-1
                                           stg.date_closed BETWEEN w.window_start_date AND w.closure_end_date
                     WHERE stg.action_closed = true
                       AND stg.continent = 'NA'
                     GROUP BY w.reporting_week_start_date
                            , stg.delivery_type
                            , stg.country
                            , stg.continent
                            , stg.abc_shortcode
                            , stg.location_code),

     TotalCases AS (SELECT w.reporting_week_start_date              as reporting_date
                         , lower(stg.delivery_type)::varchar(100)   as program_type
                         , stg.country
                         , stg.continent
                         , stg.abc_shortcode
                         , stg.location_code
                         , COUNT(*)                                 as total_actions
                    FROM WeekWindow w
                             LEFT JOIN schema_name.d_table_name stg
                                      -- ON stg.date_created BETWEEN w.window_start_date AND w.window_end_date
                                    ON stg.date_workflow_window_started BETWEEN w.window_start_date AND w.window_end_date
                    WHERE stg.action_assigned = true
                      AND stg.continent = 'NA'
                    GROUP BY w.reporting_week_start_date
                           , stg.delivery_type
                           , stg.country
                           , stg.continent
                           , stg.abc_shortcode
                           , stg.location_code)


SELECT tc.reporting_date
     , tc.program_type
     , tc.country
     , tc.continent
     , tc.abc_shortcode
     , tc.location_code
     , COALESCE(cc.closed_actions, 0)   as closed_actions
     , tc.total_actions                 as total_actions
     , CASE
           WHEN tc.total_actions = 0 THEN 0 ::numeric(6, 5)
           ELSE round((1.0 * COALESCE(cc.closed_actions, 0) / tc.total_actions), 5)::numeric(6, 5)
    END                                 as trailing_4wk_compl
FROM TotalCases tc
         LEFT JOIN ClosedCases cc
                   ON tc.reporting_date = cc.reporting_date
                       AND tc.country = cc.country
                       AND tc.continent = cc.continent
                       AND tc.abc_shortcode = cc.abc_shortcode
                       AND tc.location_code = cc.location_code
                       AND tc.program_type = cc.program_type
;



---------------------------------------------------------------------
-- raw aggregations
---------------------------------------------------------------------
drop table if exists aggregations_stage;
create temp table aggregations_stage as
select distinct

          cal.year                                  as reporting_year
        , cal.quarter                               as reporting_quarter
        , cal.month                                 as reporting_month
        , cal.week                                  as reporting_week
        , stg.week_created_start_date
    	, stg.country
    	, stg.continent
        , stg.abc_shortcode                         as abc
        , stg.program                               as provider_type
        , stg.location_code                         as location
    	, lower(stg.delivery_type)::varchar(100)    as program_type
  , sum
    (case when stg.is_workflow = true
      then 1
        else 0
     end)                                           as scenario_abc
  ,sum
    (case when stg.is_workflow = true and stg.unique_bucket = 'PROGRAM CATEGORY A'
      then 1
        else 0
     end)                                           as category_a_scenario
  ,sum
    (case when stg.is_workflow = true and stg.unique_bucket = 'PROGRAM CATEGORY B'
      then 1
        else 0
     end)                                           as category_b_scenario
  ,sum
    (case when stg.is_workflow_approved = true
      then 1
        else 0
     end)                                           as workflow_approved
  ,sum
    (case when stg.is_workflow_approved = true and stg.unique_bucket = 'PROGRAM CATEGORY A'
      then 1
        else 0
     end)                                           as category_a_workflow_approved
  ,sum
    (case when stg.is_workflow_approved = true and stg.unique_bucket = 'PROGRAM CATEGORY B'
      then 1
        else 0
     end)                                           as category_b_workflow_approved
  , sum
    (case when stg.is_duplicate = false
      then 1
        else 0
     end)                                           as total_cases_all
  ,sum
    (case
        when stg.is_third_instance = true
            and stg.permanent_action = true
                then 1
        else 0
     end)                                           as actions_z_o
  ,sum
    (case when stg.is_valid = true
    and stg.unique_bucket = 'PROGRAM CATEGORY A'
      then 1
        else 0
     end)                                           as valid_category_a
  ,sum
    (case
        when stg.unique_bucket = 'PROGRAM CATEGORY B'
            and (stg.permanent_action = true or
                 stg.permanent_actionxyz = true)
                then 1
        else 0
     end)                                           as actions_category_b
  ,sum
    (case when stg.is_valid = true
    and stg.unique_bucket = 'PROGRAM CATEGORY B'
      then 1
        else 0
     end)                                           as valid_category_b
  ,sum
    (case when stg.is_action_b = true
      then 1
        else 0
     end)                                           as action_b_count
  ,sum
    (case when stg.is_second_instance = true
      then 1
        else 0
     end)                                           as sec_ocr
  ,coalesce(sum(dtr.days_to_repeat), 0)             as days_to_repeat
  ,avg
    (case when stg.action_closed = true
      then DATEDIFF(day, stg.date_workflow_window_started, stg.date_closed)
    else null end)                                  as avg_days_to_completion
  ,sum
    (case when stg.action_closed = true
      then DATEDIFF(day, stg.date_workflow_window_started, stg.date_closed)
    else null end)                                  as days_to_completion
  ,sum
    (case when stg.action_closed = true
      then 1 else 0 end)                            as actions_resolved
  ,valid_category_a + valid_category_b              as program_violations_numerator

from schema_name.d_table_name stg

left join week_end_date cal
  on (stg.week_created_start_date = cal.week_start_date)


left join -- days to re-offend
  (select *
   from
      (select
        lag(date_created) over (partition by user_id, behavior order by date_created)  as prior
        , DATEDIFF(day, prior, date_created)        as days_to_repeat
        , *
       from schema_name.d_table_name
       where is_valid = 1)
   where is_second_instance = true or is_third_instance = true
  ) dtr
  on stg.case_number = dtr.case_number

where 1=1
and stg.week_created_start_date <= (DATE_TRUNC('week', current_date) - INTERVAL '1 WEEK')+5
and stg.program = 'ABC'
and stg.abc_shortcode is not null

group by
          cal.year
        , cal.quarter
        , cal.month
        , cal.week
        , stg.week_created_start_date
    	, stg.country
    	, stg.continent
        , stg.abc_shortcode
        , stg.location_code
    	, stg.delivery_type
        , stg.program
;



-- Salesforce records continuously change
-- Create unique ABC-location-Time pairs to produce 0 for all null metric values
-- This will avoid issues with merge upsert in downstream UDL
drop table if exists unique_time_periods;
create temp table unique_time_periods as

select   reporting_year
        , reporting_quarter
        , reporting_month
        , reporting_week
        , week_created_start_date
        , count(*) as total
from aggregations_stage
group by 1,2,3,4,5
;



-- Salesforce records continuously change
-- Create unique ABC-location-Time pairs to produce 0 for all null metric values
-- This will avoid issues with merge upsert in downstream UDL
drop table if exists unique_abc_location_pairs;
create temp table unique_abc_location_pairs as
select abc
    , location
    , country
    , continent
    , program_type
    , count(*) as total
from aggregations_stage
group by 1,2,3,4,5

;


-- Join to action data and insert 0 values for null abc-location-date pairs
DROP TABLE IF EXISTS aggregations;
CREATE TEMP TABLE aggregations AS
SELECT
    utp.reporting_year,
    utp.reporting_quarter,
    utp.reporting_month,
    utp.reporting_week,
    utp.week_created_start_date,
    uabc.country,
    uabc.continent,
    uabc.abc,
    uabc.program_type,
    -- Rare instances where action_b cases are created without a location
    -- Null location not allowed in UDL or the stored procedure below
    COALESCE(uabc.location,'UNKNOWN')               as location,
    COALESCE(ag.scenario_abc, 0)                    as scenario_abc,
    COALESCE(ag.category_a_workflow, 0)             as category_a_workflow,
    COALESCE(ag.category_b_workflow, 0)             as category_b_workflow,
    COALESCE(ag.workflow_approved, 0)               as workflow_approved,
    COALESCE(ag.category_a_workflow_approved, 0)    as category_a_workflow_approved,
    COALESCE(ag.category_b_workflow_approved, 0)    as category_b_workflow_approved,
    COALESCE(ag.total_cases_all, 0)                 as total_cases_all,
    COALESCE(ag.actions_z_o, 0)                   as actions_z_o,
    COALESCE(ag.valid_category_a, 0)                as valid_category_a,
    COALESCE(ag.actions_category_b, 0)              as actions_category_b,
    COALESCE(ag.valid_category_b, 0)                as valid_category_b,
    COALESCE(ag.action_b_count, 0)                  as action_b_count,
    COALESCE(ag.sec_ocr, 0)                         as sec_ocr,
    COALESCE(ag.days_to_repeat, 0)                  as days_to_repeat,
    COALESCE(ag.avg_days_to_completion, 0)          as avg_days_to_completion,
    COALESCE(ag.days_to_completion, 0)              as days_to_completion,
    COALESCE(ag.actions_resolved, 0)                as actions_resolved,
    COALESCE(ag.program_violations_numerator, 0)    as program_violations_numerator,
    COALESCE(nw.xyz_denominator, 0)                 as trips,
    COALESCE(tc.closed_actions, 0)                  as compl_closed,
    COALESCE(tc.total_actions, 0)                   as compl_total
FROM unique_time_periods utp
CROSS JOIN unique_abc_location_pairs uabc
LEFT JOIN aggregations_stage ag
    ON uabc.abc = ag.abc
    AND uabc.location = ag.location
    AND uabc.country = ag.country
    AND uabc.program_type = ag.program_type
    AND uabc.continent = ag.continent
    AND utp.week_created_start_date = ag.week_created_start_date
LEFT JOIN (
    SELECT
        event_week,
        CASE
            WHEN location LIKE 'A%' THEN 'GENERICNAME'
            WHEN location LIKE 'B%' THEN 'GENERICNAME'
            WHEN location LIKE 'C%' THEN 'GENERICNAME'
            ELSE 'GENERICNAME'
        END                     as program,
        country,
        region,
        abc,
        location,
        SUM(xyz_denominator)    as xyz_denominator
    FROM schema_name.fact_table_name_weekly_region
    GROUP BY 1, 2, 3, 4, 5, 6
) nw
    ON nw.event_week = utp.week_created_start_date
    AND ag.program_type = nw.program
    AND ag.country = nw.country
    AND ag.continent = nw.region
    AND ag.abc = nw.abc
    AND ag.location = nw.location
LEFT JOIN (
    SELECT
        reporting_date,
        program_type,
        country,
        continent,
        abc_shortcode,
        location_code,
        SUM(closed_actions)     as closed_actions,
        SUM(total_actions)      as total_actions
    FROM action_data
    GROUP BY 1, 2, 3, 4, 5, 6
) tc
    ON utp.week_created_start_date = tc.reporting_date
    AND uabc.program_type = tc.program_type
    AND uabc.country = tc.country
    AND uabc.continent = tc.continent
    AND uabc.abc = tc.abc_shortcode
    AND uabc.location = tc.location_code
;





drop table if exists action_data;

----------------------------------

---ABC location by Country - Overall

CALL location_ABC_Overall('case when sum(compl_total) = 0 then 0 else sum(compl_closed)*1.0/sum(compl_total) end as metric_value',
                          'sum(compl_closed) as metric_num' ,
                          'sum(compl_total) as metric_den' ,
                          '''program_completion_rate'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('case when sum(trips) = 0 then 0 else sum(valid_category_a)*100.0/sum(trips) end as metric_value',
                          'sum(valid_category_a) * 100.0 as metric_num' ,
                          'sum(trips) as metric_den' ,
                          '''program_category_a_cases_per_100_trips'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('case when sum(valid_category_a) = 0 then 0 else sum(category_a_workflow)*1.0/sum(valid_category_a) end as metric_value',
                          'sum(category_a_workflow) as metric_num' ,
                          'sum(valid_category_a) as metric_den' ,
                          '''program_category_a_workflow_rate'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('case when sum(total_cases_all) = 0 then 0 else sum(workflow)*1.0/sum(total_cases_all) end as metric_value',
                          'sum(workflow) as metric_num' ,
                          'sum(total_cases_all) as metric_den' ,
                          '''program_workflow_rate'' as metric_name',
                          'aggregations');


CALL location_ABC_Overall('case when sum(valid_category_a) = 0 then 0 else sum(sec_ocr)*1.0/sum(valid_category_a) end as metric_value',
                          'sum(sec_ocr) as metric_num' ,
                          'sum(valid_category_a) as metric_den' ,
                          '''program_Y_instance_rate'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('sum(valid_category_a)::numeric(32, 18)      as metric_value',
                            'sum(valid_category_a)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_category_a_cases_raw''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(action_b_count)::numeric(32, 18)      as metric_value',
                            'sum(action_b_count)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_action_b_count''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(program_violations_numerator)::numeric(32, 18)      as metric_value',
                            'sum(program_violations_numerator)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_violations_raw''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(valid_category_b)::numeric(32, 18)      as metric_value',
                            'sum(valid_category_b)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_category_b_cases_raw''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(actions_category_b)::numeric(32, 18)      as metric_value',
                            'sum(actions_category_b)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_actions_category_b_raw''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(workflow_approved)::numeric(32, 18)      as metric_value',
                            'sum(workflow_approved)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_workflows_approved''  as metric_name',
                            'aggregations');


CALL location_ABC_Overall('sum(actions_z_o)::numeric(32, 18)      as metric_value',
                            'sum(actions_z_o)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_actions_third_instance_raw''  as metric_name',
                            'aggregations');

CALL location_ABC_Overall('sum(category_a_workflow_approved)::numeric(32, 18)      as metric_value',
                            'sum(category_a_workflow_approved)::numeric(32, 18)  as metric_num',
                            'NULL        as metric_den',
                            '''program_category_a_workflows_approved''  as metric_name',
                            'aggregations');

CALL location_ABC_Overall('coalesce(case when sum(workflowed) = 0 then 0::int else sum(workflow_approved)*1.0/sum(workflowed) end,0) as metric_value',
                          'coalesce(sum(workflow_approved)::float,0) as metric_num' ,
                          'sum(workflowed) as metric_den' ,
                          '''program_workflow_success_rate'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('coalesce(case when sum(sec_ocr) = 0 then 0::int else sum(days_to_repeat)*1.0/sum(sec_ocr) end,0) as metric_value',
                          'coalesce(sum(days_to_repeat)::float,0) as metric_num' ,
                          'sum(sec_ocr) as metric_den' ,
                          '''program_avg_days_to_reoffend'' as metric_name',
                          'aggregations');

CALL location_ABC_Overall('coalesce(case when sum(actions_resolved) = 0 then 0::int else sum(days_to_completion)*1.0/sum(actions_resolved) end,0) as metric_value',
                          'coalesce(sum(days_to_completion)::float,0) as metric_num' ,
                          'sum(actions_resolved) as metric_den' ,
                          '''program_avg_days_to_completion'' as metric_name',
                          'aggregations');



insert into final_rollup (
select
          m.region_id
        , m.program_type
        , m.aggregation
        , m.report_period
        , m.report_date
        , m.dim_type
        , m.dim_value
        , m.metric_name
        , case
               when max(map.metric_aggregation_logic) = 'additive' then SUM(m.metric_num)
               when max(map.metric_aggregation_logic) = 'ratio' then SUM(m.metric_num) / NULLIF(SUM(m.metric_den),0)
               when max(map.metric_aggregation_logic) = 'average' then Avg(m.metric_num)
               ELSE NULL END::numeric(32, 18) as metric_value
        , case
               when max(map.metric_aggregation_logic) IN ('additive', 'ratio')
                   then SUM(m.metric_num)
               when max(map.metric_aggregation_logic) = 'average'
                   then Avg(m.metric_num)
               ELSE NULL END::numeric(32, 18) as metric_num
        , case
               when max(map.metric_aggregation_logic) IN ('additive', 'ratio')
                   then SUM(m.metric_den)
               when max(map.metric_aggregation_logic) = 'average' then Avg(m.metric_den)
               ELSE NULL END::numeric(32, 18) as metric_den
        , m.metric_updated_till_date
        , getdate()::date as insert_date
        , getdate()::date as update_date
        , m.updated_by
from
(select region_id
      , lower(fr.program_type)                                                               as program_type
      , 'ytd_month'                                                                          as aggregation
      , (to_char(fr.report_date, 'YYYY')::int * 100 +
         to_char(fr.report_date, 'MM')::int)::varchar(10)                                    as report_period
      , fr.report_date
      , lower(fr.dim_type)                                                                   as dim_type
      , fr.dim_value
      , fr.metric_name
      , sum(fr.metric_num)
        over (partition by fr.region_id, fr.program_type, fr.dim_type, fr.dim_value, fr.metric_name, left(report_period, 4)
            order by fr.report_date rows unbounded preceding)                                as metric_num
      , sum(fr.metric_den)
        over (partition by fr.region_id, fr.program_type, fr.dim_type, fr.dim_value, fr.metric_name, left(report_period, 4)
            order by fr.report_date rows unbounded preceding)                                as metric_den
      , getdate() as metric_updated_till_date
      --, max(fr.report_date) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS metric_updated_till_date
      , 'Dylan Meggs'                                                                            as updated_by
 from final_rollup fr
 where fr.aggregation = 'monthly'
 ) m
JOIN schema_name.mapping_table_name map
    ON m.region_id = map.region_id
    AND m.metric_name = map.metric_name
    AND m.dim_type = map.lowest_dim_type_granularity

group by 1,2,3,4,5,6,7,8,12,15)

;



 -----persist data
select    fr.region_id
        , lower(fr.program_type)    as program_type
        , fr.aggregation
        , fr.report_period
        , fr.report_date
        , lower(fr.dim_type)        as dim_type
        , fr.dim_value
        , fr.metric_name
        , fr.metric_value
        , fr.metric_num
        , fr.metric_den
        , max(fr.report_date) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as metric_updated_till_date
        , 'Dylan Meggs'                 as updated_by
from final_rollup fr
;


show procedure location_ABC_Overall;

/*
 CREATE OR REPLACE PROCEDURE public.location_abc_overall(metric_value character varying(65535), metric_num character varying(65535), metric_den character varying(65535), metric_name character varying(65535), table_name character varying(65535))
 LANGUAGE plpgsql
AS $$
    BEGIN
    EXECUTE '
insert into final_rollup
    (
        Select 1                                                         as Region_ID,
             ''overall''                                             as program_type,
             ''weekly''                                              as Aggregation,
             reporting_week::varchar(100)                                as Report_Period,
             wed.week_end_date                                           as report_date,
             ''country-location-abc''                                           as dim_type,
             country||''-''||location||''-''||abc                                       as dim_value,
             '||metric_name||'   ,
             '||metric_value||'  ,
             '||metric_num||'    ,
             '||metric_den||'    ,
             TO_DATE(''YYYY0123'', ''YYYYMMDD'')       as metric_updated_till_date
    	from '||table_name||' p
             left join Week_End_Date wed
                       ON p.reporting_week = wed.week
    	group by 1, 2, 3, 4, 5, 6, 7, 8
        UNION ALL
        Select 1                                                           as Region_ID,
               ''overall''                                             as program_type,
               ''monthly''                                             as Aggregation,
               (to_char(reporting_month, ''YYYY'')::int * 100 +
                to_char(reporting_month, ''MM'')::int)::varchar(100)   as Report_Period,
               date(date_trunc(''month'', reporting_month))            as report_date,
             ''country-location-abc''                                           as dim_type,
             country||''-''||location||''-''||abc                                       as dim_value,
             '||metric_name||'   ,
             '||metric_value||'  ,
             '||metric_num||'    ,
             '||metric_den||'    ,
             TO_DATE(''YYYY0123'', ''YYYYMMDD'')        as metric_updated_till_date
    	from '||table_name||'
        group by 1, 2, 3, 4, 5, 6, 7, 8
        UNION ALL
        Select 1                                                       as Region_ID,
               ''overall''                                             as program_type,
               ''quarterly''                                           as Aggregation,
               (to_char(reporting_quarter, ''YYYY'')::int * 100 +
                to_char(reporting_quarter, ''Q'')::int)::varchar(100)  as Report_Period,
               date(date_trunc(''quarter'', reporting_quarter))        as report_date,
             ''country-location-abc''                                   as dim_type,
             country||''-''||location||''-''||abc                       as dim_value,
             '||metric_name||'   ,
             '||metric_value||'  ,
             '||metric_num||'    ,
             '||metric_den||'    ,
            TO_DATE(''YYYY0123'', ''YYYYMMDD'')          as metric_updated_till_date
    	from '||table_name||'
        group by 1, 2, 3, 4, 5, 6, 7, 8
        UNION ALL
        Select 1                                                       as Region_ID,
               ''overall''                                             as program_type,
               ''yearly''                                              as Aggregation,
               to_char(reporting_year, ''YYYY'')::varchar(100)         as Report_Period,
               date(date_trunc(''year'', reporting_year))              as report_date,
             ''country-location-abc''                                   as dim_type,
             country||''-''||location||''-''||abc                       as dim_value,
             '||metric_name||'   ,
             '||metric_value||'  ,
             '||metric_num||'    ,
             '||metric_den||'    ,
             TO_DATE(''YYYY0123'', ''YYYYMMDD'')         as metric_updated_till_date
    	from '||table_name||'
        group by 1, 2, 3, 4, 5, 6, 7, 8
    );

    ';
    END
$$

 */






 -------------------------------------------------------------------------------------------------------------





/*
Description: Transform for {Name} Supplemental Report (QuickSight)
Ticket ID: https://url-where-project-documented/12345

Change Log:
YYYY-MM-DD
Date format update to avoid duplicate year when aggregating daily values
*/


drop table if exists #user_name_mapping;
create temp table #user_name_mapping  as
(
select
    -- User name mapping logic provided by authority here: https://url-here/12345
    distinct a.id as userid
    , coalesce(e.name,a.id) as name
    from datastore.schema.table_name a
    left join (select distinct  emplid, employee_first_name||' '||employee_last_name as name
    from datastore.schema.table_name) e
    on e.emplid = a.employee_id
);

-- Notes here describing the temp table
drop table if exists #base_signal_table;
create temp table #base_signal_table as
(select c.YEAR
		, c.WEEK
        , c.data_dt                     as report_date
 		, c.DS_COUNTRY_CODE				as country
 		, c.DS_CODE 					as location
 		, c.COMPANY_CODE			    as company
 		, c.workflow_status
        , c.vin
        , c.user_id
      	, d.name


 	from schema_name.table_name c
	left join #user_name_mapping d on c.user_ID=d.userid
	--Merge upsert in load: Pull only from Sunday onward to avoid partial week on weekly agg
	WHERE c.data_dt between (date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD') + 1) - interval '2 week'- 1) and TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')
	AND c.route_id IS NOT NULL
	AND LOWER(replace(c.DS_COUNTRY_CODE,' ','')) IN ('us','usa','unitedstates')
       );



SELECT
      year
    , week
    , report_date
    , country
    , location                      as location_code
    , company                       as company_shortcode
    , user_id                       as dim_id
    , name                          as dim_name
    , 'USER'                          as dim_type
    , 'Day'                         as aggregation
    , count(*)                      as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                         as compliance_rate
from #base_signal_table
group by 1,2,3,4,5,6,7,8

UNION ALL

SELECT
      year
    , week
    , report_date
    , country
    , location                          as location_code
    , company                           as company_shortcode
    , company                           as dim_id
    , 'Company Total'                   as dim_name
    , 'COMPANY'                         as dim_type
    , 'Day'                             as aggregation
    , count(*)                          as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                         as compliance_rate
from #base_signal_table
group by 1,2,3,4,5,6,7

UNION ALL

SELECT
      year
    , week
    , report_date
    , country
    , location                      as location_code
    , company                       as company_shortcode
    , vin                           as dim_id
    , 'VIN: ' || vin                as dim_name
    , 'VIN'                         as dim_type
    , 'Day'                         as aggregation
    , count(*)                      as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                         as compliance_rate
from #base_signal_table
group by 1,2,3,4,5,6,7,8

UNION ALL

-- REPEAT WITH WEEKLY AGGREGATES

SELECT
     date_part(year, rc.reporting_week_start_date)::int as year
    , b.week
    , rc.reporting_week_start_date                                                  as report_date
    , b.country
    , b.location                                                                    as location_code
    , b.company                                                                     as company_shortcode
    , b.user_id                                                                     as dim_id
    , b.name                                                                        as dim_name
    , 'USER'                                                                          as dim_type
    , 'Week'                                                                        as aggregation
    , count(*)                                                                      as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                                                                         as compliance_rate
from #base_signal_table b
join lmss_prod.dim_reporting_calendar rc
on b.report_date = rc.calendar_date
group by 1,2,3,4,5,6,7,8

UNION ALL

SELECT
     date_part(year, rc.reporting_week_start_date)::int as year
    , b.week
    , rc.reporting_week_start_date                                                  as report_date
    , b.country
    , b.location                                                                    as location_code
    , b.company                                                                     as company_shortcode
    , b.company                                                                     as dim_id
    , 'Company Total'                                                               as dim_name
    , 'COMPANY'                                                                     as dim_type
    , 'Week'                                                                        as aggregation
    , count(*)                                                                      as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                         as compliance_rate
from #base_signal_table b
join lmss_prod.dim_reporting_calendar rc
on b.report_date = rc.calendar_date
group by 1,2,3,4,5,6,7

UNION ALL

SELECT
     date_part(year, rc.reporting_week_start_date)::int as year
    , b.week
    , rc.reporting_week_start_date  as report_date
    , b.country
    , b.location                    as location_code
    , b.company                     as company_shortcode
    , b.vin                         as dim_id
    , 'VIN: ' || b.vin              as dim_name
    , 'VIN'                         as dim_type
    , 'Week'                        as aggregation
    , count(*)                      as total_stops
    , sum(case when upper(workflow_status) = 'GENERIC' then 1 else 0 end)               as column_name_here
    , sum(case when upper(workflow_status) = 'HARD CODING IS BAD' then 1 else 0 end)    as column_name_here_b
    , sum(case when upper(workflow_status) <> 'BUT SOMETIMES IT MUST BE DONE' then 1 else 0 end)   as column_name_here_c
    , sum(case when upper(workflow_status) = 'SO DO IT WISELY' then 1 else 0 end)       as column_name_here_d
    , case
        when column_name_here_c = round((0 * 1.00000),5)
            then null
        else round((column_name_here * 1.0 / column_name_here_c),5)
        end                         as compliance_rate
from #base_signal_table b
join lmss_prod.dim_reporting_calendar rc
on b.report_date = rc.calendar_date
group by 1,2,3,4,5,6,7,8
;


--------------------------------------------------------------------------------------------------------------------

 /*
 Change Log:


 */

 -----------Should set run date and depedency as Saturday-----------

 -----------This gets weekly routes from XYZ_SOURCE-----------

 drop table if exists source;
 create temp table source sortkey (event_date) distkey (user_id)
 as
 select * from schema_name.table_name
          where country in ('US', 'CA') and event_date >= 'YYYY-MM-DD';

 -----------This gets all events from XYZ_EVENTS and maps to ABC eligibility-----------
 drop table if exists abc_eligible_numerator;
 create temp table abc_eligible_numerator sortkey (event_date) distkey (user_id)
 as
 with base as (
 select data_source,
        event_id,
        event_date,
        event_start_time_utc,
        event_end_time_utc,
        event_start_time_pst,
        event_end_time_pst,
        event_duration,
        vehicle_id,
        source_id,
        nvl(source, 'NA') as source,
        nvl(type, 'NA') as type,
        additional_attributes,
        nvl(subtype, 'NA') as subtype,
        nvl(severity, 'NA') as severity,
        nvl(subseverity, 'NA') as subseverity,
        nvl(source_event_type, 'NA') as source_event_type,
        mode,
        vehicle_speed,
        speed_units,
        speed_limit,
        video_status,
        acme_status,
        dispute_submission_date,
        ineligible_submission,
        dispute_review_date,
        proactive_review_date,
        proactive_review_result,
        final_resolution,
        final_comments,
        version_id,
        user_id,
        d2v_source,
        company,
        provider_id,
        location,
        country,
        xyz_eligible_flag,
        xyz_impact_flag,
        display_to_company,
        confidence_score,
        video_url,
        xyz_metric,
        source_match_flag,
        vehicle_ownershiptype,
        event_weight,
        display_resolution,
        locations,
        start_location,
        end_location,
        start_latitude,
        end_latitude,
        start_longitude,
        end_longitude
        from schema_name.table_name
        where
            xyz_impact_flag = 1 and
            country in ('US', 'CA') and
            event_date >= 'YYYY-MM-DD')

 select
     base.*,
     nvl(abc.abc_eligible, 0) as abc_numerator_eligible
     from base
     left join schema_name.mapping_table_name abc on
     base.country = abc.country and
     base.source = abc.source and
     base.type = abc.type and
     base.subtype = abc.subtype and
     base.severity = abc.severity and
     base.subseverity = abc.subseverity and
     base.source_event_type = abc.source_event_type;


 -----------This pulls x to ensure y based on z -----------
 select
 	TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYY-MM-DD') as snapshot_date,
     calendar.calendar_year,
     calendar.calendar_month_of_year,
     calendar.calendar_qtr,
     calendar.reporting_week_of_year,
     a.*, --Replace this with actual list of columns
     source.business,
     source.category_source,
     source.categoryb_source,
     source.categoryc_source,
     source.categoryd_source,
     source.categorye_source,
     source.ntrd_eligible,
     source.xy_eligible,
     source.vehicle_ownership,
     source.vehicle_make,
     source.vehicle_model,
     source.vehicle_year,
     source.ntrd_installed,
     source.xy_installed,
     source.ntrd_cam_obstructed,
     source.xy_faulty_category_sensor,
     case when a.source = source.category_source then 1 else 0 end as category_eligible,
     case when a.source = source.categoryb_source then 1 else 0 end as categoryb_eligible,
     case when a.source = source.categoryc_source then 1 else 0 end as categoryc_eligible,
     case when a.source = source.categoryd_source then 1 else 0 end as categoryd_eligible,
     case when a.source = source.categorye_source then 1 else 0 end as categorye_eligible

     from abc_eligible_numerator a
     left join datastore.schema.calendar_table_name calendar
         on calendar.calendar_day = a.event_date
     left join source on a.user_id = source.user_id and
                         a.event_date = source.event_date and
                         a.vehicle_id = source.vin and
                         a.country = source.country
;





--------------------------------------------------------------------------------------------------------------------



/*
 Change Log:


 */


 --------------------------------------Reporting Layer Weekly--------------------------------------
 drop table if exists #business_abc_weekly;
 create temp table #business_abc_weekly as
 with
 business_denominator as (
     select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3

 union all

     select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3),

 business_numerator as (
 select
     next_day(event_date, 'Sun')-1 as reporting_day,
     country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3

 union all

 select
     next_day(event_date, 'Sun')-1 as reporting_day,
     'NA' as country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3)

 select
     a.reporting_day,
     a.country,
     --business,
     metric_type,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     business_numerator a
     left join business_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country and
        a.metric_type = b.business;


 drop table if exists #network_abc_weekly;
 create temp table #network_abc_weekly as

 with network_denominator as (
     select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2

 union all

     select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' AS country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2),

 temp as (
 -----------Network Metric----------
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'WEEKLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all

 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'WEEKLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7


 -----------XYZ Metric----------
 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------Type----------
 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------subtype----------
 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------subseverity----------
 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     next_day(event_date, 'Sun')-1 as reporting_day, --week_end_date
     'NA' as country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7)

 select
     a.reporting_day,
     a.country,
     a.alert_category,
     a.alert_subcategory,
     a.alert_severity,
     a.alert_severity_subtype,
     a.metric_name,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     temp a
     left join network_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country;


 --select  to_char(TO_DATE('2024-01-01', 'YYYY-MM-DD'), 'IYYYIW')


 --------------------------------------Reporting Layer Monthly--------------------------------------
 drop table if exists #business_abc_monthly;
 create temp table #business_abc_monthly as
 with
 business_denominator as (
     select
     date_trunc('month', event_date) as reporting_day,
     country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3

 union all

     select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3),

 business_numerator as (
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3

 union all

 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3)

 select
     a.reporting_day,
     a.country,
     --business,
     metric_type,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     business_numerator a
     left join business_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country and
        a.metric_type = b.business;


 drop table if exists #network_abc_monthly;
 create temp table #network_abc_monthly as

 with network_denominator as (
     select
     date_trunc('month', event_date) as reporting_day,
     country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2

 union all

     select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2
                                   ),

 temp as (
 -----------Network Metric----------
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'MONTHLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all

 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'MONTHLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7


 -----------XYZ Metric----------
 union all
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------Type----------
 union all
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------subtype----------
 union all
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

  -----------subseverity----------
 union all
 select
     date_trunc('month', event_date) as reporting_day,
     country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('month', event_date) as reporting_day,
     'NA' as country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

     )


 select
     a.reporting_day,
     a.country,
     a.alert_category,
     a.alert_subcategory,
     a.alert_severity,
     a.alert_severity_subtype,
     a.metric_name,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     temp a
     left join network_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country;




 --------------------------------------Reporting Layer Quarterly--------------------------------------
 drop table if exists #business_abc_quarterly;
 create temp table #business_abc_quarterly as
 with
 business_denominator as (
     select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3

 union all
     select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3),

 business_numerator as (
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3

 union all

 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3)

 select
     a.reporting_day,
     a.country,
     --business,
     metric_type,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     business_numerator a
     left join business_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country and
        a.metric_type = b.business;


 drop table if exists #network_abc_quarterly;
 create temp table #network_abc_quarterly as

 with network_denominator as (
     select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2

 union all

     select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2),

 temp as (
 -----------Network Metric----------
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'QUARTERLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all

 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'QUARTERLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------XYZ Metric----------
 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------Type----------
 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------subtype----------
 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

  -----------subseverity----------
 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('quarter', event_date)::date as reporting_day,
     'NA' as country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7)


 select
     a.reporting_day,
     a.country,
     a.alert_category,
     a.alert_subcategory,
     a.alert_severity,
     a.alert_severity_subtype,
     a.metric_name,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     temp a
     left join network_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country;




 --------------------------------------Reporting Layer Yearly--------------------------------------
 drop table if exists #business_abc_yearly;
 create temp table #business_abc_yearly as
 with
 business_denominator as (
     select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3

 union all

     select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     business,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA')
     group by 1,2,3),

 business_numerator as (
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3

 union all

 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     business as metric_type,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD' group by 1,2,3)

 select
     a.reporting_day,
     a.country,
     --business,
     metric_type,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     business_numerator a
     left join business_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country and
        a.metric_type = b.business;


 drop table if exists #network_abc_yearly;
 create temp table #network_abc_yearly as

 with network_denominator as (
     select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2

 union all

     select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     count(*) abc_denominator
     from schema_name.table_name source
     where acme_eligible = 1
       and event_date >= 'YYYY-MM-DD'
       and country in ('US', 'CA') group by 1,2),

 temp as (
 -----------Network Metric----------
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'YEARLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all

 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     'ALL' as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'YEARLY ALL' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7


 -----------XYZ Metric----------
 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     xyz_metric as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'xyz_metric' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------Type----------
 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     type as alert_category,
     '' as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'type' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 -----------subtype----------
 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     '' as alert_category,
     subtype as alert_subcategory,
     '' as alert_severity,
     '' as alert_severity_subtype,
     'subtype' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

  -----------subseverity----------
 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7

 union all
 select
     date_trunc('year', event_date)::date as reporting_day,
     'NA' as country,
     'HARD-CODED' as alert_category,
     subtype as alert_subcategory,
     severity as alert_severity,
     subseverity as alert_severity_subtype,
     'subseverity' as metric_name,
     sum(abc_numerator_eligible) abc_numerator
     from schema_name.abc_table_name
     where abc_numerator_eligible = 1 and type = 'HARD-CODED'
       and event_date >= 'YYYY-MM-DD'
     group by 1,2,3,4,5,6,7
     )


 select
     a.reporting_day,
     a.country,
     a.alert_category,
     a.alert_subcategory,
     a.alert_severity,
     a.alert_severity_subtype,
     a.metric_name,
     abc_numerator,
     abc_denominator,
     abc_numerator/abc_denominator::decimal*100 as abc_rate
     from
     temp a
     left join network_denominator b
     on a.reporting_day = b.reporting_day and
        a.country = b.country;


 --------------------------FINAL UNION ALL--------------------------
 drop table if exists #final_transform;
 create temp table #final_transform as
 select
     reporting_day,
     'weekly' as aggregation,
     reporting_year*100+reporting_week_of_year as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     'weekly_abc_by_type_or_subtype' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #network_abc_weekly nsw
     left join datastore.schema.calendar_table_name calendar on
         nsw.reporting_day = calendar.calendar_day


 union all

 select
     reporting_day,
     'weekly' as aggregation,
     reporting_year*100+reporting_week_of_year as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     metric_type as alert_category,
     'ALL' as alert_subcategory,
     'ALL' as alert_severity,
     'ALL' as alert_severity_subtype,
     'Program' as metric_name,
     'weekly_abc_by_program' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #business_abc_weekly bsw
     left join datastore.schema.calendar_table_name calendar on
         bsw.reporting_day = calendar.calendar_day

 union all


 select
     reporting_day,
     'monthly' as aggregation,
     calendar_year*100+calendar_month_of_year as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     'monthly_abc_by_type_or_subtype' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #network_abc_monthly nsm
     left join datastore.schema.calendar_table_name calendar on
         nsm.reporting_day = calendar.calendar_day


 union all

 select
     reporting_day,
     'monthly' as aggregation,
     calendar_year*100+calendar_month_of_year as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     metric_type as alert_category,
     'ALL' as alert_subcategory,
     'ALL' as alert_severity,
     'ALL' as alert_severity_subtype,
     'Program' as metric_name,
     'monthly_abc_by_program' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #business_abc_monthly bsm
     left join datastore.schema.calendar_table_name calendar on
         bsm.reporting_day = calendar.calendar_day


 union all


 select
     reporting_day,
     'quarterly' as aggregation,
     calendar_year*100+calendar_qtr as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     'quarterly_abc_by_type_or_subtype' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #network_abc_quarterly nsq
     left join datastore.schema.calendar_table_name calendar on
         nsq.reporting_day = calendar.calendar_day


 union all

 select
     reporting_day,
     'quarterly' as aggregation,
     calendar_year*100+calendar_qtr as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     metric_type as alert_category,
     'ALL' as alert_subcategory,
     'ALL' as alert_severity,
     'ALL' as alert_severity_subtype,
     'Program' as metric_name,
     'quarterly_abc_by_program' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #business_abc_quarterly bsq
     left join datastore.schema.calendar_table_name calendar on
         bsq.reporting_day = calendar.calendar_day


 union all


 select
     reporting_day,
     'yearly' as aggregation,
     calendar_year*100 as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     'yearly_abc_by_type_or_subtype' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #network_abc_yearly nsy
     left join datastore.schema.calendar_table_name calendar on
         nsy.reporting_day = calendar.calendar_day


 union all

 select
     reporting_day,
     'yearly' as aggregation,
     calendar_year*100 as report_period,
     country as country_code,
     'ALL' as company,
     'ALL' as location,
     'ALL' as user_id,
     'ABC' as mechanism,
     metric_type as alert_category,
     'ALL' as alert_subcategory,
     'ALL' as alert_severity,
     'ALL' as alert_severity_subtype,
     'Program' as metric_name,
     'yearly_abc_by_program' as metric_name_explained,
     abc_rate as metric_value,
     abc_numerator as metric_num,
     abc_denominator as metric_den
     from #business_abc_yearly bsy
     left join datastore.schema.calendar_table_name calendar on
         bsy.reporting_day = calendar.calendar_day;


 select
     reporting_day,
     aggregation,
     report_period,
     country_code,
     company,
     location,
     user_id,
     mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     metric_name_explained,
     metric_value,
     metric_num,
     metric_den,
     CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS insert_timestamp_pt
 from #final_transform
 ;



--------------------------------------------------------------------------------------------------------------------

/*
 Change Log:


 */

 -----------Should set run date and depedency as Saturday-----------

 -----------This gets weekly routes from XYZ_SOURCE-----------

 select
     reporting_day,
     aggregation,
     report_period,
     country_code,
     company,
     location,
     user_id,
     mechanism,
     alert_category,
     alert_subcategory,
     alert_severity,
     alert_severity_subtype,
     metric_name,
     metric_name_explained,
     metric_value,
     metric_num,
     metric_den
     from schema_name.abc_reporting_layer
     where insert_timestamp_pt = (select max(insert_timestamp_pt) from schema_name.abc_reporting_layer)



----------------------------------------------------------------------------------------------------------------------



