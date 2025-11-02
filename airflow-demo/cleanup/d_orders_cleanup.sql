DROP TABLE IF EXISTS staging.d_orders_staging;

/*
Wait for the load to run before dropping the staging table.

Note that you may want to set depends_on_past = true in the dag to avoid collisions when running the 
same workflow multiple times simultaneously, such as with backfilling or re-running executions after a pipeline failure.

You may want to keep the staging table, depending on your use case.
We already have raw data in S3, and we're creating a final dimension table, so I don't retain the staging table in this example.
*/