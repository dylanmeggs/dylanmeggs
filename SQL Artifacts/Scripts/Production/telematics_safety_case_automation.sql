/*+ ETLM {
    depend:{
       add:[
           {name:"org_schema.o_am_employment_na"},
           {name:"org_schema.na_widget_scorecard_v3" ,cutoff:{hours:2}},
           {name:"org_analytics.oss_events_daily"}
        ]
    }
}*/


/*
Change Log:
2023-10-26 @dylan
This design was created with speed prioritized over scalability. We needed to launch within x days to avoid customer-facing issues.
I would like to create more mapping tables to move these hard coded values to a centralized location. 

2023-09-05 @dylan
OSSP3 updates = Consolidating unload jobs + switching to new tables + creating mapping tables

TICKET LINK: https://ticket-url.xyz.com/ticket-number
 */



------------------------------------------------------------------------------------------------
-------This section pulls speed information for events which impact org scorecard---------------
------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS speeding_event_filter;
CREATE TEMPORARY TABLE speeding_event_filter DISTKEY
(
transporter_id
) SORTKEY
(
event_date
) AS

SELECT DISTINCT
    transporter_id
    , vehicle_id
    , event_id
    , event_duration
    , subtype
, case
    when sign(vehicle_speed) >= 0
        then vehicle_speed ::numeric(10,7)
    else null
    end as vehicle_speed_filtered
, case
    when sign(speed_limit) >= 0
        then speed_limit::numeric(10,7)
    else null
    end as speed_limit_filtered
, ROUND(((vehicle_speed_filtered - speed_limit_filtered)::numeric(10,7)),2)::numeric(5,2) as speed_difference
, source
, event_date
FROM org_analytics.oss_events_daily
where type IN ('SPEEDING', 'SPEEDING VIOLATION')
AND speed_limit <> 'N/A'
and vehicle_speed <> 'N/A'
and speed_difference IS NOT NULL
and oss_impact_flag = true
and event_date = (TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
and sign(speed_difference) = 1
and country = 'US'
-- the logic above fails query due to data type mismatch.
-- There should be no data type mismatch, so this must be due to order of operations or some other backend process.
;



------------------------------------------------------------------------------------------------
---------------This section aggregates daily records for behavior groupings---------------------
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS oss_events_transposed_na;
CREATE TEMPORARY TABLE oss_events_transposed_na DISTKEY
(
ACME_transporter_id
) SORTKEY
(
event_date
) AS

SELECT
--Confidential query... unable to redact
;



------------------------------------------------------------------------------------------------
---------------This section uses additonal layers of logic to filter results--------------------
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS oss_da_daily_final;
CREATE TEMPORARY TABLE oss_da_daily_final as

SELECT
--Confidential query... unable to redact

FROM oss_events_transposed_na oss;





------------------------------------------------------------------------------------------------
-------------------------------This section pulls event-level data------------------------------
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS base_{TEMPORARY_TABLE_SEQUENCE};
CREATE TEMP TABLE base_{TEMPORARY_TABLE_SEQUENCE} AS

SELECT DISTINCT oss.type
              , oss.additional_attributes
              , oss.subtype
              , oss.severity
              , oss.subseverity
              , oss.vehicle_id
              , oss.event_date AS day
              , coalesce(oss.video_url, oss.event_id) as alert_sf_value
              , len(alert_sf_value) as alert_sf_value_length
              , oss.event_id
              , oss.transporter_id
              , oss.source
              , oss.source_event_type
              , oss.station
              , oss.org
              , oss.oss_metric -- This value can be used in the future to simplify case statements and where clauses
                               -- I will refrain from using it until we've had time to research, plan, test, etc.
                               
FROM org_analytics.oss_events_daily oss

WHERE oss.event_date = (TO_DATE('{RUN_DATE_YYYYMMDD}', 'YYYYMMDD'))
  AND oss_impact_flag = true
  AND country = 'US'
;



------------------------------------------------------------------------------------------------
------------------------Creates the final table for the Salesforce Unload-----------------------
------------------------------------------------------------------------------------------------

/*
CR NOTE:
The original query creates a separate salesforce case for each vin.
I think it makes sense to instead listagg vin and partition the alert_id by transporter id.
I will consider changing this in a future update. Current goal is to get the query running under OSSP3 framework.
 */


DROP TABLE IF EXISTS final_{TEMPORARY_TABLE_SEQUENCE};
CREATE TEMP TABLE final_{TEMPORARY_TABLE_SEQUENCE} AS
SELECT --High Severity Stop Sign
    DISTINCT INITCAP(pn.name)                               AS dr_name
     , oss.station                                          AS station
     , oss.org                                              AS org
     , 'Mapped to secondary case type in Salesforce'        AS breach_type
     , 'TEAM'                                               AS case_origin
     , oss.program                                          AS Program
     , 'Mapped to primary case type in Salesforce'          AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                    AS alert_id
     , oss.ACME_transporter_id                              AS transid
     , oss.vin                                              AS vin
     , ae.ACMEemployeeid                                    AS sample_id
     , oss.event_date                                       AS breach_date
     , getdate()::date                                      AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) as rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.program_stop_sign >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'SIGN-VIOLATIONS'

UNION ALL

SELECT --High Severity Stop Light
    DISTINCT INITCAP(pn.name)                               AS dr_name
     , oss.station                                          AS station
     , oss.org                                              AS org
     , 'Mapped to secondary case type in Salesforce'        AS breach_type
     , 'TEAM'                                               AS case_origin
     , oss.program                                          AS Program
     , 'Mapped to primary case type in Salesforce'          AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                    AS alert_id
     , oss.ACME_transporter_id                              AS transid
     , oss.vin                                              AS vin
     , ae.ACMEemployeeid                                    AS sample_id
     , oss.event_date                                       AS breach_date
     , getdate()::date                                      AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) as rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE oss.stop_light_major_events >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'TRAFFIC-LIGHT-VIOLATION'
  AND b.subtype = 'SEVERE EXAMPLE'
--b.additional_attributes like '{"severity":"SEVERE"%"subtype":"Red - Major"%'

UNION ALL

SELECT --No Stop Detected
    DISTINCT INITCAP(pn.name)                               AS dr_name
     , oss.station                                          AS station
     , oss.org                                              AS org
     , 'Mapped to secondary case type in Salesforce'        AS breach_type
     , 'TEAM'                                               AS case_origin
     , oss.program                                          AS Program
     , 'Mapped to primary case type in Salesforce'          AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
       WITHIN GROUP (ORDER BY b.transporter_id)
       OVER (PARTITION BY b.vehicle_id)                     AS alert_id
     , oss.ACME_transporter_id                              AS transid
     , oss.vin                                              AS vin
     , ae.ACMEemployeeid                                    AS sample_id
     , oss.event_date                                       AS breach_date
     , getdate()::date                                      AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) as rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE oss.stop_sign_no_stop_events >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'SIGN-VIOLATIONS'
  AND b.subtype = 'SPECIFIC EXAMPLE'

UNION ALL

SELECT --Distracted Driving
    DISTINCT INITCAP(pn.name)                               AS dr_name
     , oss.station                                          AS station
     , oss.org                                              AS org
     , 'Mapped to secondary case type in Salesforce'        AS breach_type
     , 'TEAM'                                               AS case_origin
     , oss.program                                          AS Program
     , 'Mapped to primary case type in Salesforce'          AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                    AS alert_id
     , oss.ACME_transporter_id                              AS transid
     , oss.vin                                              AS vin
     , ae.ACMEemployeeid                                    AS sample_id
     , oss.event_date                                       AS breach_date
     , getdate()::date                                      AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) AS rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.distractions_rate_events >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'user-DISTRACTION'
  AND b.subtype IN ('Specific example', 'created by third party', 'in collaboration with us')

UNION ALL

SELECT --No Seatbelt Detected
    DISTINCT INITCAP(pn.name)                           AS dr_name
     , oss.station                                      AS station
     , oss.org                                          AS org
     , 'Mapped to secondary case type in Salesforce'    AS breach_type
     , 'TEAM'                                           AS case_origin
     , oss.program                                      AS Program
     , 'Mapped to primary case type in Salesforce'      AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                AS alert_id
     , oss.ACME_transporter_id                          AS transid
     , oss.vin                                          AS vin
     , ae.ACMEemployeeid                                AS sample_id
     , oss.event_date                                   AS breach_date
     , getdate()::date                                  AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) AS rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.seatbelt_off_rate_events >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'SAMPLE-SIGNAL'
  AND b.subtype = 'Sample text'
  --AND b.source = 'ThirdParty'
  --As of 2023-09-06, ThirdPartyB daily rate is capped at x. No ThirdPartyB source records should populate here.

UNION ALL

SELECT --Secret signal type
    DISTINCT INITCAP(pn.name)           AS dr_name
     , oss.station                      AS station
     , oss.org                          AS org
     , 'Mapped to secondary case type in Salesforce'      AS breach_type
     , 'TEAM'                                             AS case_origin
     , oss.program                                        AS Program
     , 'Mapped to primary case type in Salesforce'        AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)    AS alert_id
     , oss.ACME_transporter_id        AS transid
     , oss.vin                          AS vin
     , ae.ACMEemployeeid              AS sample_id
     , oss.event_date                   AS breach_date
     , getdate()::date                  AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) AS rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.sample_example >= --CONFIDENTIAL THRESHOLD
  AND b.type = 'SECRET-SIGNAL'
  AND b.subtype IN ('CONDITION A', 'CONDITION B', 'CONDITION C', 'CONDITION D')



UNION ALL

SELECT --3+ Occurrences of Speeding 10+ MPH for 5 seconds CURRENT TEAM THRESHOLD
    DISTINCT INITCAP(pn.name)                             AS dr_name
     , oss.station                                        AS station
     , oss.org                                            AS org
     , 'Mapped to secondary case type in Salesforce'      AS breach_type
     , 'TEAM'                                             AS case_origin
     , oss.program                                        AS Program
     , 'Mapped to primary case type in Salesforce'        AS Bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                  AS alert_id
     , oss.ACME_transporter_id                            AS transid
     , oss.vin                                            AS vin
     , ae.ACMEemployeeid                                  AS sample_id
     , oss.event_date                                     AS breach_date
     , getdate()::date                                    AS load_date

FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) AS rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id
LEFT JOIN speeding_event_filter sef
    ON b.event_id = sef.event_id
    and b.transporter_id = sef.transporter_id
    and b.vehicle_id = sef.vehicle_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.team_speeding_a >= --CONFIDENTIAL THRESHOLD
  AND b.oss_metric = 'SPEEDING'
  AND sef.event_duration >= --CONFIDENTIAL THRESHOLD
  AND (sef.speed_difference >= --CONFIDENTIAL THRESHOLD
        OR b.source_event_type = 'Unique Scorecard Speeding Event 1')



UNION ALL

-- ThirdParty HIgh Sev Speeding
SELECT DISTINCT
    INITCAP(pn.name)                                    AS dr_name
    , oss.station                                       AS station
    , oss.org                                           AS org
    , 'High Severity Speeding'                          AS breach_type
    , 'TEAM'								            AS case_origin
    , oss.program                                       AS program
    , 'TEAM High Sev'						            AS bucket
     , listagg(DISTINCT b.event_id, ', ')
        WITHIN GROUP (ORDER BY b.transporter_id)
        OVER (PARTITION BY b.vehicle_id)                AS alert_id
     , oss.ACME_transporter_id                          AS transid
     , oss.vin                                          AS vin
     , ae.ACMEemployeeid                                AS sample_id
     , oss.event_date                                   AS breach_date
     , getdate()::date                                  AS load_date
FROM oss_da_daily_final oss
JOIN org_schema.o_am_employment_na ae
    ON oss.ACME_transporter_id = ae.personid
LEFT JOIN
     (SELECT transporter_id
           , name
           , rank() over (partition by transporter_id order by last_updated_date desc) AS rnk
      FROM org_schema.na_widget_scorecard_v3
      WHERE name is not null) pn
    ON oss.ACME_transporter_id = pn.transporter_id
    AND rnk = 1
JOIN base_{TEMPORARY_TABLE_SEQUENCE} b
    ON oss.vin = b.vehicle_id
    AND oss.ACME_transporter_id = b.transporter_id
LEFT JOIN speeding_event_filter sef
    ON b.event_id = sef.event_id
    and b.transporter_id = sef.transporter_id
    and b.vehicle_id = sef.vehicle_id

WHERE ae.ACMEemployeeid IS NOT NULL
  AND oss.team_high_sev_speeding_ntrd >= --CONFIDENTIAL THRESHOLD
  AND b.type IN ('SPEEDING', 'SPEEDING VIOLATION')
  AND b.source = 'ThirdParty'
  AND sef.vehicle_speed_filtered >= --CONFIDENTIAL THRESHOLD

;



------------------------------------------------------------------------------------------------
------------------------Pulls the unfiltered results for email publishing-----------------------
------------------------------------------------------------------------------------------------

SELECT *
FROM final_{TEMPORARY_TABLE_SEQUENCE};

------------------------------------------------------------------------------------------------
----------------------------This section is for the Salesforce Unload---------------------------
------------------------------------------------------------------------------------------------

Unload('SELECT * FROM final_{TEMPORARY_TABLE_SEQUENCE}')
to 's3://bucket-location/file_name_{RUN_DATE_YYYYMMDD}_'
iam_role 'arn:aws:iam::123456789:role/redshift_access_S3_role,arn:aws:iam::123456789:role/org-data-analytics-team'
ALLOWOVERWRITE
CSV
header
region 'us-west-2'
parallel off;