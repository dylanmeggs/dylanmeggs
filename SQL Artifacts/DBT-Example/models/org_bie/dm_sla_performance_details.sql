{{
    config(
         sort='date_created'
    )
}}

/*
This code, along with the other tables in this directory, are examples spun off of my team's original data 
infrastructure, which I helped build. We spun up a data mart with dbt and mapping tables in AWS data lake. 
This was later deprecated in favor of internal orchestration tools. 

We also moved all of our data into different schemas when the org restructured and our BIE+DE teams 
consolidated. Integrating the two teams was as simple as deprecating the data mart, changing naming 
conventions, and updating permissions.
*/
WITH owner_queues AS (SELECT UPPER(NULLIF(TRIM(owner), '')) AS owner_queue,
                             MIN(transfer_sla)              AS transfer_sla,
                             MIN(email_sla)                 AS email_sla,
                             MIN(resolved_sla)              AS resolved_sla
                      FROM {{ source('org_bie', 'dl_sla_owner_queues') }}
                      WHERE owner_queue IS NOT NULL
                      GROUP BY owner_queue),
     emails AS (SELECT ch.case_id,
                       ch.date_created
                FROM {{ ref('dm_sf_case_history_log') }} AS ch
                WHERE (ch.field = 'Status'
                    AND ch.new_value = 'IN APPEAL WINDOW')
                   OR (ch.field = 'Communication_Status__c'
                    AND ch.new_value = 'WAITING FOR RESPONSE')
                GROUP BY ch.case_id, ch.date_created),
     resolved AS (SELECT ch.case_id,
                         MIN(ch.date_created) AS date_resolved
                  FROM {{ ref('dm_sf_case_history_log') }} AS ch
                  WHERE (ch.field = 'Status'
                      AND ch.new_value IN ('RESOLVED', 'CLOSED'))
                  GROUP BY ch.case_id),
     queues AS (SELECT ch.case_id,
                       ch.new_value                                                            AS queue,
                       MIN(ch.date_created)                                                    AS date_entered,
                       LEAD(date_entered, 1) OVER (PARTITION BY case_id ORDER BY date_entered) AS date_transferred
                FROM {{ ref('dm_sf_case_history_log') }} AS ch
                         INNER JOIN owner_queues AS oq
                                    ON ch.new_value = oq.owner_queue
                WHERE ch.field IN ('ownerAssignment', 'Owner')
                GROUP BY ch.case_id, ch.new_value)

SELECT ded.case_number,
       q.queue,
       COALESCE(dsqt.team, ded.reviewing_team, 'UNKNOWN') AS team,
       ded.origin,
       ded.date_created,
       drc.reporting_week                                 AS week_created,
       drc.reporting_month                                AS month_created,
       drc.reporting_quarter                              AS quarter_created,
       drc.reporting_year                                 AS year_created,
       q.date_entered,
       q.date_transferred,
       r.date_resolved,
       MIN(e.date_created)                                AS date_emailed,
       DATEDIFF(HOUR, q.date_entered, q.date_transferred) AS hours_until_transferred,
       DATEDIFF(HOUR, q.date_entered, date_emailed)       AS hours_until_emailed,
       DATEDIFF(HOUR, q.date_entered, r.date_resolved)    AS hours_until_resolved,
       CASE
           WHEN hours_until_transferred <= COALESCE(oq.transfer_sla, -1)
               OR hours_until_emailed <= COALESCE(oq.email_sla, -1)
               OR hours_until_resolved <= COALESCE(oq.resolved_sla, -1)
               THEN 1
           ELSE 0
           END                                            AS is_sla_met,
       LEAST(hours_until_transferred - oq.transfer_sla, hours_until_emailed - oq.email_sla,
             hours_until_resolved - oq.resolved_sla)      AS sla_delta
FROM queues AS q
         INNER JOIN {{ ref('dm_escalation_details') }} AS ded
                    ON q.case_id = ded.id
                        AND ded.is_closed = TRUE
         LEFT JOIN {{ ref('dm_reporting_calendar') }} AS drc
                   ON ded.date_created = drc.calendar_date
         LEFT JOIN owner_queues AS oq
                   ON q.queue = oq.owner_queue
         LEFT JOIN resolved AS r
                   ON q.case_id = r.case_id
         LEFT JOIN emails AS e
                   ON q.case_id = e.case_id
                       AND e.date_created BETWEEN q.date_entered AND COALESCE(q.date_transferred,
                                                                              CURRENT_TIMESTAMP AT TIME ZONE 'america/los_angeles')
         LEFT JOIN {{ source('org_bie', 'dl_sla_queue_teams') }} AS dsqt
                   ON q.queue = dsqt.queue
WHERE drc.reporting_year >= (SELECT start_year FROM {{ ref('calc_variables') }} LIMIT 1)
GROUP BY ded.case_number, q.queue, dsqt.team, ded.reviewing_team, ded.origin, ded.date_created, drc.reporting_week,
         drc.reporting_month, drc.reporting_quarter, drc.reporting_year, q.date_entered, q.date_transferred,
         r.date_resolved, oq.transfer_sla, oq.email_sla, oq.resolved_sla