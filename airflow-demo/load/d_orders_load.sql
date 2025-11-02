BEGIN;

MERGE INTO prod.d_orders AS target
USING staging.d_orders_staging AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        customer_id = source.customer_id,
        order_timestamp = source.order_timestamp,
        total_amount = source.total_amount,
        order_status = source.order_status,
        updated_at = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        order_timestamp,
        total_amount,
        order_status,
        created_at,
        updated_at
    )
    VALUES (
        source.order_id,
        source.customer_id,
        source.order_timestamp,
        source.total_amount,
        source.order_status,
        GETDATE(),
        GETDATE()
    );

COMMIT;

/*
Note: If unable to use Redshift, you can replicate the merge upsert this way...

DELETE FROM mart.d_orders
USING staging.d_orders_staging
WHERE mart.d_orders.order_id = staging.d_orders_staging.order_id;

INSERT INTO mart.d_orders
SELECT * FROM staging.d_orders_staging;
*/
