
SELECT
       ssct.id,
       ssct.status                                                                  AS current_account_status,

       'Date: ' || COALESCE(TRIM(ssct.date_last_modified), '') :: TEXT  || ' | ' ||
       'Status: ' || COALESCE(TRIM(ssct.operational_status), '')        || ' | ' ||
       'By: ' || COALESCE(TRIM(ssct.last_modified_by), '')              || ' | ' ||
       'Type: ' || COALESCE(TRIM(ssct.last_modified_by_type), '')                   AS details
FROM secret_schema.cool_table AS ssct
WHERE id = '';