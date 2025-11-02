-- Create a staging table for cleaned and shaped data
-- Cannot use temp table if separating the extract and transform, because each Airflow task is a unique session
-- If you do not want a staging table, then you can consider combining the extract and transform into one file / query
CREATE TABLE IF NOT EXISTS staging.d_orders_staging AS
SELECT
    order_id::VARCHAR(50) AS order_id,
    customer_id::VARCHAR(50) AS customer_id,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
    UPPER(order_status) AS order_status,
    source_file
FROM spectrum.raw_orders  -- Glue/Spectrum external table
WHERE order_timestamp >= dateadd(day, -1, current_date);
