-- Get vin from tid
SELECT DISTINCT
      ta.registrationnumber        as license_plate_number 
    , tb.vehicle_id                as vin_number
    , tb.transporter_id
    , tb.local_date
    , tb.start_timestamp           as start_timestamp_utc
    , tb.end_timestamp             as end_timestamp_utc
    , tb.state
FROM secret_schema.table_a_na ta
JOIN secret_schema.table_b_na tb
ON tb.vin = ta.vehicle_id
AND tb.state = ta.state

WHERE transporter_id = 'id-here'
ORDER BY start_timestamp DESC