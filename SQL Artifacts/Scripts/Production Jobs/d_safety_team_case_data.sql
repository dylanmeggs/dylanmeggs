/*+ ETLM {
     depend:{
         add:[
             {name:"schema_prod.o_raw_ticket_table",age:{hours:24}}
         ]
	 }
  }*/

CREATE TEMP TABLE base_{TEMPORARY_TABLE_SEQUENCE} AS

SELECT
		  UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(details, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT 				AS summary
        , UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(ticket_issue_alias, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT   AS ticket_issue_alias
        , resolved_date :: DATE																														AS resolved_date
        , create_date   :: DATE                                                       																AS program_date_created
    	, UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(category, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT				AS category
    	, UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(resolution, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT			AS resolution
        , UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(root_cause, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT			AS root_cause
        , UPPER(NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(root_cause_details, '\\t', ''), '\\\\n', ''), '\\\\', '')), '')) :: TEXT   AS root_cause_details
        , UPPER(root_cause_details)																												    AS root_cause_unfiltered
FROM chema_prod.o_raw_ticket_table
    WHERE (root_cause LIKE '%TEST%' OR root_cause LIKE '%program%' OR root_cause LIKE '%Category%') 
    AND (category = 'program' OR category = 'LM-TEST');


WITH filter AS (

SELECT
			CASE
               WHEN root_cause_details LIKE '%DATE OF INCIDENT:%'
                   THEN SUBSTRING(NULLIF(LTRIM(TRIM(SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(root_cause_unfiltered, 'DATE OF INCIDENT:', 2), ' ', ''), '\\\\N', ' '), ' ', 1)), ':'), ''), 1, 24) :: TEXT
               WHEN root_cause_details LIKE '%INCIDENT DATE:%'
                   THEN SUBSTRING(NULLIF(LTRIM(TRIM(SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(root_cause_unfiltered, 'INCIDENT DATE:', 2), ' ', ''), '\\\\N', ' '), ' ', 1)), ':'), ''), 1, 24) :: TEXT
               ELSE NULL
               END                                                                         		AS program_incident_date,
           program_date_created,    
           TRIM(',$#: ' FROM
                SPLIT_PART(LTRIM(SPLIT_PART(root_cause_details, 'TRACKING ID:', 2)), ' ', 1))    AS tracking_number_unrefined,
            CASE
                WHEN root_cause_details like '%TID:%'
                    THEN
            TRIM(',$#: ' FROM
                SPLIT_PART(LTRIM(SPLIT_PART(root_cause_details, 'TID:', 2)),
                           ' ', 1))
                WHEN root_cause_details like '%TRANSPORTER ID:%'
                    THEN
           TRIM(',$#: ' FROM
                SPLIT_PART(LTRIM(SPLIT_PART(root_cause_details, 'TRANSPORTER ID:', 2)), ' ', 1))
                           ELSE NULL
                           END                                                          AS transporter_id_unrefined,
           CASE
               WHEN tracking_number_unrefined IN ('N/A', 'UNKNOWN', 'NA', 'UNKOWN', 'NULL', 'NONE', '', ' ') THEN NULL
               WHEN tracking_number_unrefined LIKE 'TB%TB' THEN SUBSTRING(tracking_number_unrefined, 1, 15)
               ELSE tracking_number_unrefined
               END                                                                         AS tracking_number,

           CASE
               WHEN transporter_id_unrefined IN ('N/A', 'UNKNOWN', 'NA', 'UNKOWN', 'NULL', 'NONE', '', ' ') THEN NULL
               ELSE transporter_id_unrefined
               END                                                                         AS transporter_id,
           CASE
               WHEN root_cause_details LIKE '%CID:%'
                   THEN NULLIF(TRIM(',$#: ' FROM
                                    SPLIT_PART(LTRIM(SPLIT_PART(root_cause_details, 'CID:', 2)), ' ', 1)), '')
               WHEN root_cause_details LIKE '%CUSTOMER ACCOUNT ID:%'
                   THEN NULLIF(TRIM(',$#: ' FROM SPLIT_PART(
                        LTRIM(SPLIT_PART(root_cause_details, 'CUSTOMER ACCOUNT ID:', 2)), ' ', 1)), '')
               WHEN root_cause_details LIKE '%CUSTOMER ID:%'
                   THEN NULLIF(TRIM(',$#: ' FROM SPLIT_PART(
                        LTRIM(SPLIT_PART(root_cause_details, 'CUSTOMER ID:', 2)), ' ', 1)), '')
               ELSE NULL
               END                                                                         AS customer_id,
           CONCAT('https://ticket.abc.acme.com/', ticket_issue_alias)                           AS ticket_issue_link,
           category,
           resolution,
           resolved_date,
           root_cause,
           root_cause_details,
           summary
FROM base_{TEMPORARY_TABLE_SEQUENCE}
WHERE (transporter_id IS NOT NULL OR customer_id IS NOT NULL)
    			)
 
SELECT
		  NULLIF(program_incident_date, '') AS program_incident_date
        , program_date_created
    	, tracking_number
    	, transporter_id
    	, customer_id
   		, ticket_issue_link
    	, category
    	, resolution
        , resolved_date
        , root_cause
        , root_cause_details
        , summary
      
FROM filter