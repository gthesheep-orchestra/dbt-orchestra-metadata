{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates operation counts by time buckets and operation types, returning counts of succeeded, warning, and failed operations. It also ensures that empty buckets are returned when there are no operation types.
*/


WITH bucket_operation_counts AS (
    SELECT
        ab.time_from,
        ab.time_to,
        dot.operation_type,
        COUNT(CASE WHEN od.status = 'succeeded' THEN 1 END) AS succeeded_count,
        COUNT(CASE WHEN od.status = 'warning' THEN 1 END) AS warning_count,
        COUNT(CASE WHEN od.status = 'failed' THEN 1 END) AS failed_count
    FROM
        {{ ref('all_buckets') }} AS ab
    CROSS JOIN
        {{ ref('distinct_operation_types') }} AS dot
    LEFT JOIN
        {{ ref('operation_data') }} AS od ON od.inserted_at >= ab.time_from
        AND od.inserted_at < ab.time_to + INTERVAL '1 microseconds'
        AND od.operation_type = dot.operation_type
    GROUP BY
        ab.time_from,
        ab.time_to,
        dot.operation_type
    UNION ALL
    -- Return empty buckets when there are no operation types
    SELECT
        ab.time_from,
        ab.time_to,
        NULL::text AS operation_type,
        0 AS succeeded_count,
        0 AS warning_count,
        0 AS failed_count
    FROM
        {{ ref('all_buckets') }} AS ab
    WHERE NOT EXISTS (SELECT 1 FROM {{ ref('distinct_operation_types') }})
)
SELECT
    bucket_operation_counts.time_from,
    bucket_operation_counts.time_to,
    bucket_operation_counts.operation_type,
    COALESCE(bucket_operation_counts.succeeded_count, 0) AS succeeded_count,
    COALESCE(bucket_operation_counts.warning_count, 0) AS warning_count,
    COALESCE(bucket_operation_counts.failed_count, 0) AS failed_count
FROM
    bucket_operation_counts
ORDER BY
    bucket_operation_counts.time_from,
    bucket_operation_counts.operation_type;

