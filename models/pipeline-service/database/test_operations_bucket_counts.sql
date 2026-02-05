{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates test operation counts by time intervals, providing a summary of succeeded, warning, and failed operations within specified time buckets.
*/


WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        generate_series + INTERVAL ':bucket_size minute' AS time_to,
        COUNT(CASE WHEN test_operation.operation_status = 'succeeded' THEN 1 END) AS succeeded_count,
        COUNT(CASE WHEN test_operation.operation_status = 'warning' THEN 1 END) AS warning_count,
        COUNT(CASE WHEN test_operation.operation_status = 'failed' THEN 1 END) AS failed_count
    FROM
        (SELECT
            generate_series(:time_from, :time_to, INTERVAL ':bucket_size minute') AS generate_series
        ) AS time_intervals
    LEFT JOIN {{ ref('test_operation') }} AS test_operation ON test_operation.inserted_at >= time_intervals.generate_series
        AND test_operation.inserted_at < time_intervals.generate_series + INTERVAL ':bucket_size minute'
    GROUP BY
        time_from,
        time_to
)
SELECT
    time_from,
    time_to,
    succeeded_count,
    warning_count,
    failed_count,
    (SELECT SUM(succeeded_count) FROM bucket_counts) AS total_succeeded,
    (SELECT SUM(warning_count) FROM bucket_counts) AS total_warning,
    (SELECT SUM(failed_count) FROM bucket_counts) AS total_failed
FROM
    bucket_counts
ORDER BY
    time_from;

