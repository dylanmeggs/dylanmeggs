/*+ ETLM { 
   depend:{ 
       add:[ 

               {name:"org_spectrum_ext.d_team_widget_na"},
               {name:"schema_na.d_team_operations_widget_na",age:{hours:48}},
               {name:"schema_ext.team.d_team_widget_events_na"}
         ] 
   }  
}*/




/*
Business Owners: stakeholder@ + otherstakeholder@
Purpose: This query does x,y,z with the purpose of automatically identifying and responding to behaviors which are consistent with 
signature fraud. The criteria used to determine this is based off of available inputs and legally approved thresholds.
 */

/*
Change Log
2023-05-19 @dwmeggs - Removed ALLOWOVERWRITE from unload statement to prevent the creation of duplicate Salesforce cases. 
*/

/*
Change Log
2023-09-07 @teammember - Replaced "old table name" with "new table name" that is subsribed using new ingestion pipeline
*/


DROP TABLE IF EXISTS acme;
CREATE TEMPORARY TABLE acme DISTKEY(scannable_id) as (
	select distinct
    	TRUNC(EVENT_DATETIME) as event_date,
    	event_day,
		scannable_id,
		transporter_id,
		provider_company_short_code as org,
    	transport_request_id,
    	fulfillment_transport_id,
    	destination_address_id as address_id,
    	ordering_order_id,
		delivery_station_country_code AS country,
    	delivery_station_code as station,
    	transport_reason,
        CASE
		WHEN BUSINESS_TYPE = 'ACME' THEN 'ACME'
		WHEN BUSINESS_TYPE IN ('Widget', 'ACME X') THEN 'ABC'
		WHEN BUSINESS_TYPE IS NULL THEN 'NULL'
		ELSE business_type END AS BUSINESS_TYPE,
		 CASE
		    WHEN PROVIDER_TYPE = 'org' THEN 'DA'
		    WHEN PROVIDER_TYPE = 'AmWidget' THEN 'DP'
    		WHEN PROVIDER_TYPE = 'IHX' THEN 'IHX'
    		WHEN PROVIDER_TYPE IS NULL THEN 'NULL'
    		ELSE PROVIDER_TYPE
	    END AS PROVIDER_TYPE
	FROM
		schema_ext.team.d_team_widget_events_na as a
	where
		1 = 1
		AND event_day >= date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 8
    	AND event_day < date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 1
		AND transport_status in ('DELIVERED')
    and 
        (
            transport_type = 'Delivery'
					or transport_pickup_instructions is NULL
        )
    AND is_latest_by_transport_status_and_eventdate = 'Y'
    AND transport_request_instructions LIKE '%REQUIRE_SIGNATURE%'
	AND a.provider_type = 'org'
	AND a.business_type = 'ACME'
	
);




DROP TABLE IF EXISTS ABC_ACME;
create temporary table ABC_ACME as (
	SELECT
		DISTINCT 
		a.tracking_id, 
		a.fulfillment_transport_id,
    	a.aid,
		a.delivery_date,
		a.min_concession_date as abc_date,
		a.conceded_units,
		a.total_units_in_pkg,
		CASE WHEN a.conceded_units = b.total_units_in_pkg THEN 'Y' END AS UNITS,
    	b.total_pkg_price,
    	b.total_pkg_price_base_currency_code,
		1 as abc
	FROM
		(
		    SELECT
		    tracking_id,
			fulfillment_transport_id,
			delivery_date,
			aid,
			min_concession_date,
		    SUM(concession_units) as conceded_units
		 FROM   
		(
			SELECT
				tracking_id,
				fulfillment_transport_id,
				DATE(actual_delivery_datetime) as delivery_date,
				address_id as aid,
				concession_units,
				MIN(DATE(reporting_date)) OVER (PARTITION BY tracking_id, fulfillment_transport_id) as min_concession_date,
				CASE WHEN (	MIN(DATE (reporting_date)) OVER (PARTITION BY tracking_id, fulfillment_transport_id)) = DATE (reporting_date) THEN 'Y' END AS first_date,
	            CASE WHEN (	MIN(DATE (reporting_date)) OVER (PARTITION BY tracking_id, fulfillment_transport_id) - DATE (actual_delivery_datetime)) <= 45 THEN 'Y' END AS date_diff
			FROM schema_na.d_team_operations_widget_na
			WHERE actual_delivery_datetime >= date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 8
    			AND actual_delivery_datetime < date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 1
				AND is_active = 'Y'
				AND owner_type = 'Carrier'
				AND is_ACME = 'Y'
				AND concession_bucket_l1 = 'Delivered Not Received'
		) 
		WHERE 
		    1 = 1 
		    AND first_date = 'Y'
			AND date_diff = 'Y'
		GROUP BY 
		1,2,3,4,5
		) as a
		LEFT JOIN (
			SELECT
				DISTINCT 
				tracking_id,
				fulfillment_transport_id,
				total_units_in_pkg,
            total_pkg_price,
            total_pkg_price_base_currency_code
			FROM org_spectrum_ext.d_team_widget_na
			where
				1 = 1
				and ship_day >= date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 15
    			AND ship_day < date_trunc('week', TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')) - 1
            -- Ship day is "The timestamp at which the transport left the first node"
            -- This could be several days prior to the delivery
		) b ON b.tracking_id = a.tracking_id
		AND b.fulfillment_transport_id = a.fulfillment_transport_id
        WHERE UNITS = 'Y'
);




DROP TABLE IF EXISTS acme_abc;
create TEMPORARY table acme_abc DISTKEY(scannable_id) as (
	SELECT distinct
		ore_ACME.event_date,
    	address_id,
		ore_ACME.scannable_id,
		ore_ACME.station,
		ore_ACME.transporter_id,
		ore_ACME.org,
		ore_ACME.country,
		ore_ACME.ordering_order_id,
		ore_ACME.transport_reason,
		ore_ACME.business_type,
    	ore_ACME.fulfillment_transport_id,
    	total_pkg_price,
    	total_pkg_price_base_currency_code,
    	PROVIDER_TYPE,
    	abc_date,
		MAX(
			CASE
				WHEN abc.xyz IS NULL THEN 0
				ELSE 1
			END
		) AS abc_flag
	FROM
    (SELECT * FROM acme) as ore_ACME
    left join ABC_ACME abc 
		on ore_ACME.scannable_id = abc.tracking_id
		AND ore_ACME.fulfillment_transport_id = abc.fulfillment_transport_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    );
    


-- Orchestration tool requires that I wrap count distinct for use within listagg
DROP TABLE IF EXISTS final_{TEMPORARY_TABLE_SEQUENCE};
create TEMPORARY table final_{TEMPORARY_TABLE_SEQUENCE} as (

select 
    rc.reporting_week_start_date
  , a.transporter_id
  , pn.name as da_name
  , a.station
  , a.org
  , listagg(a.scannable_id, ', ') as tracking_id
  , count(a.scannable_id) as total_instances

from
(select * from acme_abc where abc_flag = 1 ) a
join schema_prod.dim_reporting_calendar rc
    on a.event_date ::DATE = rc.calendar_date
   
    JOIN
        (SELECT DISTINCT
            transporter_id
            , name
            , rank() over (partition by transporter_id order by last_updated_date desc) as rnk
        FROM schema_name.table_with_no_dependency_check_needed
        WHERE name is not null) pn
      ON a.transporter_id = pn.transporter_id
      AND rnk = 1
    
GROUP BY 1,2,3,4,5
ORDER BY 1,2,4,5   
);

SELECT * from final_{TEMPORARY_TABLE_SEQUENCE};


/*-- Gamma unload
Unload('SELECT * FROM final_{TEMPORARY_TABLE_SEQUENCE}')
to 's3://bucket-location/file_name_{RUN_DATE_YYYYMMDD}_'
IAM_ROLE 'arn:aws:iam::123456789:role/redshift_access_S3_role,arn:aws:iam::123456789:role/org-data-analytics-team'
ALLOWOVERWRITE
CSV
header
region 'us-east-2'
parallel off;*/



/*-- Prod
Unload('SELECT * FROM final_{TEMPORARY_TABLE_SEQUENCE}')
to 's3://bucket-location-prod/file_name_{RUN_DATE_YYYYMMDD}_'
iam_role 'arn:aws:iam::123456789:role/redshift_access_S3_role,arn:aws:iam::123456789:role/org-data-analytics-team'
CSV
header
region 'us-west-2'   
parallel off;*/