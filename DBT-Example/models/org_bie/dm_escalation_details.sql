{{
    config(
         sort='date_created'
    )
}}

SELECT ded.case_number,
       ded.tracking_number,
       ded.transporter_id,
       ded.record_type_id,
       ded.cms_record_id,
       ded.data_source,
       ded.program,
       ded.case_record_type,
       ded.origin,
       ded.source_of_feedback,
       ded.ev_type,
       ded.ev_strength,
       ded.delivery_type,
       ded.not_an_infraction_reason,
       ded.behavior,
       ded.behavior_category,
       ded.behavior_bucket,
       ded.status,
       ded.da_status,
       ded.resolution_code,
       ded.cms_category,
       ded.user_s_queue,
       ded.scorecard_week,
       ded.business_coach,
       ded.reviewing_team,
       ded.station_org_pair,
       ded.station_code,
       ded.org_shortcode,
       ded.org_name,
       ded.da_name,
       ded.description,
       ded.communication_status,
       ded.user_defect_total,
       ded.scheduled_event_update,
       ded.date_created,
       ded.date_closed,
       ded.date_last_modified,
       ded.date_of_incident,
       ded.time_of_incident,
       ded.date_appeal_window_ended,
       ded.date_appeal_window_started,
       ded.dataset_day,
       ded.is_closed,
       ded.is_deleted,
       ded.has_appeal_received,
       ded.has_acknowledgement_received,
       ded.was_first_response_missed,
       ded.was_sla_missed,
       ded.was_first_response_met,
       ded.was_sla_met,
       ded.key,
       ded.internal_annotations,
       ded.has_internal_annotations,
       ded.subject,
       ded.id,
       ded.user_type,
       rc_created.reporting_year                          AS year_created,
       rc_created.reporting_month_name_abbr               AS month_created,
       rc_created.reporting_week                          AS week_created,
       rc_created.reporting_week_start_date               AS week_created_start_date,
       rc_created.current_reporting_week_offset           AS weeks_since_week_created,
       rc_closed.reporting_year                           AS year_closed,
       rc_closed.reporting_month_name_abbr                AS month_closed,
       rc_closed.reporting_week                           AS week_closed,
       rc_closed.reporting_week_start_date                AS week_closed_start_date,
       rc_appeal_window_started.reporting_year            AS year_appeal_window_started,
       rc_appeal_window_started.reporting_month_name_abbr AS month_appeal_window_started,
       rc_appeal_window_started.reporting_week            AS week_appeal_window_started,
       rc_appeal_window_started.reporting_week_start_date AS week_appeal_window_started_start_date,
       COALESCE(dsd.country, ded.country)                 AS country,
       COALESCE(dsd.continent, ded.continent)             AS continent,
       parent_id,
       is_xworkflow_org_offboarded,
       is_xworkflow_completion_offboarded,
       tier,
       ded.status = 'RESOLVED'
           AND (
           -- violation
               (
                       ded.behavior_bucket IN ('VIOLATION', 'CODE XYZ') AND
                       ded.resolution_code IN ('CODE ABC', 'CODE XYZ')
                   )
               -- defect
               OR (
                       ded.behavior_bucket IN ('DEFECT') AND
                       ded.resolution_code IN ('CODE A', 'CODE XYZ')
                   )
               -- XYZ
               OR (
                   ded.behavior_bucket = 'XYZ' AND ded.resolution_code = 'NOT A XYZ'
                   )
               -- xworkflow/XYZ
               OR (
                   -- daf
                       (ded.behavior_bucket = 'xworkflow' AND ded.resolution_code = 'NOT A xworkflow') OR
                       -- rsi
                       (ded.behavior_bucket IN ('CODE ABC RETRAINING', 'XYZ') AND
                        ded.resolution_code IN
                        ('NOT A xworkflow', 'NOT A XYZ', 'NOT A CODE ABC RETRAINING', 'CODE ABC', 'CODE A'))
                   )
               -- other
               OR (
                   ded.resolution_code = 'COACHING REQUEST'
                   )
           )                                              AS is_not_an_issue,
       ded.behavior_bucket IN ('VIOLATION', 'DEFECT', 'XYZ', 'xworkflow', 'XYZ', 'CODE ABC RETRAINING') AND
       ded.date_appeal_window_started IS NOT NULL         AS is_org_notified,
       ded.status = 'RESOLVED'
           AND (
           -- violation
               ((ded.data_source <> 'SALESFORCE'
                    OR ded.da_status = 'VIOLATION OFFBOARD') AND
                ded.resolution_code IN
                ('NO RESPONSE', 'APPEAL REJECTED', 'NON APPEAL RESPONSE', 'org OFFBOARDED')
                   AND ded.behavior_bucket = 'VIOLATION')
               -- defect
               OR (ded.resolution_code IN
                   ('NO RESPONSE', 'APPEAL REJECTED', 'ACKNOWLEDGEMENT RECEIVED')
               AND ded.behavior_bucket = 'DEFECT')
               -- XYZ
               OR (ded.behavior_bucket = 'XYZ' AND (ded.data_source <> 'SALESFORCE' OR ded.da_status = 'XYZ - OFFBOARD'))
               -- xworkflow/XYZ
               OR (
                   --daf
                       (ded.behavior_bucket = 'xworkflow' AND
                        ded.resolution_code IN ('xworkflow TRAINING ACCEPTED', 'NO DSP RESPONSE')) OR
                       -- rsi
                       (ded.behavior_bucket IN ('CODE ABC RETRAINING', 'XYZ') AND
                        ded.resolution_code IN
                        ('xworkflow TRAINING ACCEPTED', 'NO RESPONSE', 'CODE ABC RETRAINING TRAINING ACCEPTED',
                         'XYZ TRAINING ACCEPTED'))
                   )
               OR (
                   -- coaching
                    ded.behavior_bucket = 'COACHING' AND ded.resolution_code = 'COACHING REQUEST'
               )
           )                                              AS is_valid,
       has_appeal_received = TRUE AND
       ded.behavior_bucket IN ('VIOLATION', 'DEFECT')     AS is_appeal,
       ded.status = 'RESOLVED'
           AND ded.resolution_code = 'APPEAL APPROVED'
           AND
       ded.behavior_bucket IN ('VIOLATION', 'DEFECT')     AS is_appeal_approved,
       -- violation/XYZ
       (is_valid = TRUE AND ded.behavior_bucket IN ('VIOLATION', 'XYZ'))
           -- xworkflow
           OR (ded.status = 'RESOLVED' AND ded.behavior_bucket IN ('xworkflow', 'CODE ABC RETRAINING', 'XYZ') AND
               (ded.is_xworkflow_org_offboarded = TRUE OR
                ded.is_xworkflow_completion_offboarded = TRUE)) AS is_offboard,

       ded.behavior_bucket IN ('xworkflow', 'CODE ABC RETRAINING', 'XYZ') AND
       ded.date_appeal_window_started IS NOT NULL         AS is_suspension,
       ded.behavior_bucket IN ('xworkflow', 'CODE ABC RETRAINING', 'XYZ') AND
       ded.resolution_code IN ('xworkflow TRAINING ACCEPTED', 'CODE ABC RETRAINING TRAINING ACCEPTED',
                               'XYZ TRAINING ACCEPTED')   AS is_retraining_activation,
       CASE
           WHEN is_valid = TRUE AND ded.behavior_bucket IN ('VIOLATION')
               AND COALESCE(ded.not_an_infraction_reason, '') <> 'org SELF-REPORTED' THEN 3.0
           WHEN is_valid = TRUE AND ded.behavior_bucket IN ('DEFECT')
               AND (ded.date_created < '2021-09-15' OR ded.tier = 'TIER-2') THEN 1.0
           WHEN is_valid = TRUE AND ded.behavior_bucket IN ('XYZ')
               -- after 9/15/21 additional logic was added for XYZs in CED calculation
               AND (ded.date_created < '2021-09-15' OR
                    ded.resolution_code IN ('DEFECT RECEIVED POST COMPLETION OF xworkflow RETRAINING')) THEN 2.0
           -- only applicable after 9/15/21 (xworkflow)
           WHEN
                   ded.date_created >= '2021-09-15' AND
                   is_valid = TRUE AND ded.behavior_bucket IN ('xworkflow') THEN 1.5
           END                                            AS customer_escalation_defect_value,
           customer_account_id,
            datetime_created,
            datetime_closed,
            datetime_last_modified,
            datetime_appeal_window_ended,
            datetime_appeal_window_started,
            lp_owner,
            cms_record_url,
            amp.cohort_week                                    AS transporter_cohort_week,
            amp.cohort_month                                   AS transporter_cohort_month,
            amp.cohort_quarter                                 AS transporter_cohort_quarter,
            amp.cohort_year                                    AS transporter_cohort_year,
            DATEDIFF(WEEK, amp.date_hired,
                        ded.date_created) + 1                     AS transporter_tenure,
            CASE
                WHEN transporter_tenure <= 4 THEN '4 WEEKS'
                WHEN transporter_tenure <= 8 THEN '8 WEEKS'
                WHEN transporter_tenure <= 16 THEN '16 WEEKS'
                WHEN transporter_tenure > 16 THEN '17+ WEEKS'
                ELSE 'UNKNOWN' END                             AS transporter_tenure_bucket,
            rc_incident.reporting_week_start_date               AS incident_week_date,
            DATEDIFF(WEEK, ddt.creation_date, ded.date_of_incident) + 1 as org_tenure,
            CASE
               WHEN org_tenure <= 4 THEN '4 WEEKS'
               WHEN org_tenure <= 8 THEN '8 WEEKS'
               WHEN org_tenure <= 16 THEN '16 WEEKS'
               WHEN org_tenure > 16 THEN '16+ WEEKS'
               ELSE 'UNKNOWN'
               END                                                                 AS org_tenure_bucket,
            CASE
                WHEN  datediff(WEEK, ddt.ThirdParty_install_date, incident_week_date) + 1 > 0
                    THEN datediff(WEEK, ddt.ThirdParty_install_date, incident_week_date) + 1
                ELSE 0 END                                                         AS org_ThirdParty_tenure,
            CASE
                WHEN org_ThirdParty_tenure <= 0 THEN 'UNKNOWN'
                WHEN ddt.ThirdParty_install_date <= '2021-02-19' THEN 'PRE-02/19/2021'
                WHEN org_ThirdParty_tenure <= 4 THEN '4 WEEKS'
                WHEN org_ThirdParty_tenure <= 8 THEN '8 WEEKS'
                WHEN org_ThirdParty_tenure <= 16 THEN '16 WEEKS'
                WHEN org_ThirdParty_tenure > 16 THEN '16+ WEEKS'
                ELSE 'UNKNOWN' END                     AS org_ThirdParty_bucket
FROM (SELECT case_number,
             tracking_number,
             transporter_id,
             record_type_id,
             cms_record_id,
             data_source,
             program,
             case_record_type,
             origin,
             source_of_feedback,
             ev_type,
             ev_strength,
             delivery_type,
             not_an_infraction_reason,
             behavior,
             behavior_category,
             behavior_bucket,
             status,
             da_status,
             resolution_code,
             cms_category,
             user_s_queue,
             scorecard_week,
             business_coach,
             reviewing_team,
             station_org_pair,
             station_code,
             org_shortcode,
             org_name,
             da_name,
             description,
             communication_status,
             user_defect_total,
             scheduled_event_update,
             date_created,
             date_closed,
             date_last_modified,
             date_of_incident,
             time_of_incident,
             date_appeal_window_ended,
             date_appeal_window_started,
             dataset_day,
             is_closed,
             is_deleted,
             has_appeal_received,
             has_acknowledgement_received,
             was_first_response_missed,
             was_sla_missed,
             was_first_response_met,
             was_sla_met,
             key,
             internal_annotations,
             has_internal_annotations,
             subject,
             id,
             'DA' AS user_type,
             'US' AS country,
             'NA' AS continent,
             parent_id,
             is_xworkflow_org_offboarded,
             is_xworkflow_completion_offboarded,
             tier,
             customer_account_id,
             datetime_created,
             datetime_closed,
             datetime_last_modified,
             datetime_appeal_window_ended,
             datetime_appeal_window_started,
             lp_owner,
             cms_record_url
      FROM {{ ref('calc_salesforce_escalation_details') }}

      UNION ALL

      SELECT case_number,
             tracking_number,
             transporter_id,
             record_type_id,
             cms_record_id,
             data_source,
             program,
             case_record_type,
             origin,
             source_of_feedback,
             ev_type,
             ev_strength,
             delivery_type,
             not_an_infraction_reason,
             behavior,
             behavior_category,
             behavior_bucket,
             status,
             da_status,
             resolution_code,
             cms_category,
             user_s_queue,
             scorecard_week,
             business_coach,
             reviewing_team,
             station_org_pair,
             station_code,
             org_shortcode,
             org_name,
             da_name,
             description,
             communication_status,
             user_defect_total,
             scheduled_event_update,
             date_created,
             date_closed,
             date_last_modified,
             date_of_incident,
             time_of_incident,
             date_appeal_window_ended,
             date_appeal_window_started,
             dataset_day,
             is_closed,
             is_deleted,
             has_appeal_received,
             has_acknowledgement_received,
             was_first_response_missed,
             was_sla_missed,
             was_first_response_met,
             was_sla_met,
             key,
             internal_annotations,
             has_internal_annotations,
             subject,
             id,
             user_type,
             'US' AS country,
             'NA' AS continent,
             parent_id,
             is_xworkflow_org_offboarded,
             is_xworkflow_completion_offboarded,
             tier,
             customer_account_id,
             datetime_created,
             datetime_closed,
             datetime_last_modified,
             datetime_appeal_window_ended,
             datetime_appeal_window_started,
             lp_owner,
             cms_record_url
      FROM {{ ref('calc_na_org_ticketing_escalation_details') }}
      WHERE date_created < '2021-02-14'

      UNION ALL

      SELECT case_number,
             tracking_number,
             transporter_id,
             record_type_id,
             cms_record_id,
             data_source,
             program,
             case_record_type,
             origin,
             source_of_feedback,
             ev_type,
             ev_strength,
             delivery_type,
             not_an_infraction_reason,
             behavior,
             behavior_category,
             behavior_bucket,
             status,
             da_status,
             resolution_code,
             cms_category,
             user_s_queue,
             scorecard_week,
             business_coach,
             reviewing_team,
             station_org_pair,
             station_code,
             org_shortcode,
             org_name,
             da_name,
             description,
             communication_status,
             user_defect_total,
             scheduled_event_update,
             date_created,
             date_closed,
             date_last_modified,
             date_of_incident,
             time_of_incident,
             date_appeal_window_ended,
             date_appeal_window_started,
             dataset_day,
             is_closed,
             is_deleted,
             has_appeal_received,
             has_acknowledgement_received,
             was_first_response_missed,
             was_sla_missed,
             was_first_response_met,
             was_sla_met,
             key,
             internal_annotations,
             has_internal_annotations,
             subject,
             id,
             user_type,
             CASE
                 WHEN UPPER(category_calc) LIKE '%US%' OR
                      UPPER(category_calc) = 'LMTOC - ACME Widget'
                     THEN 'US'
                 WHEN UPPER(category_calc) LIKE '%CA%' THEN 'CA'
                 WHEN UPPER(category_calc) LIKE '%UK%' THEN 'GB'
                 WHEN UPPER(category_calc) LIKE '%ES%' THEN 'ES'
                 WHEN UPPER(category_calc) LIKE '%DE%' THEN 'DE'
                 WHEN UPPER(category_calc) LIKE '%SG%' THEN 'SG'
                 WHEN UPPER(category_calc) LIKE '%JP%' THEN 'JP'
                 WHEN UPPER(category_calc) LIKE '%AU%' THEN 'AU'
                 WHEN UPPER(category_calc) LIKE '%IN%' THEN 'IN'
                 ELSE 'US'
                 END AS country,
             CASE
                 WHEN UPPER(country) IN ('US', 'CA', 'MX') THEN 'NA'
                 WHEN UPPER(country) IN ('UK', 'GB', 'DE', 'ES', 'FR', 'NL', 'IT', 'AT') THEN 'EU'
                 WHEN UPPER(country) IN ('JP', 'AU', 'SG', 'IN') THEN 'FE'
                 END AS continent,
             parent_id,
             is_xworkflow_org_offboarded,
             is_xworkflow_completion_offboarded,
             tier,
             customer_account_id,
             datetime_created,
             datetime_closed,
             datetime_last_modified,
             datetime_appeal_window_ended,
             datetime_appeal_window_started,
             lp_owner,
             cms_record_url
      FROM {{ ref('calc_ww_Widget_ticketing_escalation_details') }}) AS ded
         LEFT JOIN {{ ref('dm_reporting_calendar') }} AS rc_created
                   ON ded.date_created = rc_created.calendar_date
         LEFT JOIN {{ ref('dm_reporting_calendar') }} AS rc_closed
                   ON ded.date_closed = rc_closed.calendar_date
         LEFT JOIN {{ ref('dm_reporting_calendar') }} AS rc_appeal_window_started
                   ON ded.date_appeal_window_started = rc_appeal_window_started.calendar_date
         LEFT JOIN {{ ref('dm_station_details') }} AS dsd
                   ON ded.station_code = dsd.station_code
         LEFT JOIN (SELECT id                  AS transporter_id,
                           MIN(date_hired)     AS date_hired,
                           MIN(cohort_week)    AS cohort_week,
                           MIN(cohort_month)   AS cohort_month,
                           MIN(cohort_quarter) AS cohort_quarter,
                           MIN(cohort_year)    AS cohort_year
                    FROM {{ ref('dm_am_person_details') }}
                    WHERE is_current_record = TRUE
                    GROUP BY id) AS amp
                   ON ded.transporter_id = amp.transporter_id
         LEFT JOIN {{ ref('dm_reporting_calendar') }} AS rc_incident
                   ON ded.date_of_incident = rc_incident.calendar_date
        LEFT JOIN {{ ref('dm_org_tenure_details')}}  AS ddt
                    ON (ded.org_shortcode = ddt.shortcode AND ded.continent = ddt.region)