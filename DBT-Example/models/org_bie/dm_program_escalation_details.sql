{{
    config(
         sort='date_created'
    )
}}

SELECT ded.key,
       ded.case_number,
       ded.transporter_id,
       ded.da_name,
       ded.da_status,
       ded.station_code,
       ded.org_shortcode,
       ded.org_name,
       CASE
           WHEN ded.behavior IN ('TEST',
                                 'TEST2',
                                 'TEST3')
               THEN 'EXAMPLE OUTPUT'
           ELSE ded.behavior
           END AS behavior,
       ded.behavior_category,
       ded.behavior_bucket,
       ded.status,
       ded.resolution_code,
       ded.date_created,
       ded.date_closed,
       ded.date_appeal_window_started,
       ded.date_appeal_window_ended,
       ded.dataset_day,
       ded.is_closed,
       ded.not_an_infraction_reason,
       ded.internal_annotations,
       CASE 
            WHEN ded.delivery_type = 'TEST' THEN 'TEST'
            ELSE 'TEST'
        END AS delivery_type,
        ded.origin,
        ded.country,
        ded.continent,
        ded.year_created,
        ded.month_created,
        ded.week_created,
        ded.week_created_start_date,
        transporter_cohort_week,
        transporter_cohort_month,
        transporter_cohort_quarter,
        transporter_cohort_year,
        transporter_tenure,
       transporter_tenure_bucket,
       ded.org_tenure,
       ded.org_tenure_bucket,
       ded.org_ThirdParty_tenure,
       ded.org_ThirdParty_bucket
FROM {{ ref('dm_escalation_details') }} AS ded
WHERE ded.data_source = 'SALESFORCE'
  AND ded.case_record_type = 'EXAMPLE TYPE'
  AND ded.origin IN ('ORIGINA', 'ORIGINB')