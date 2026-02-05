{{
  config(
    materialized='table'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates operation counts by distinct operation types over specified time buckets.
*/

WITH generate_series AS (
    SELECT *
    FROM GENERATE_SERIES(
        {{ var('time_from') }},
        {{ var('time_to') }},
        INTERVAL '{{ var('bucket_size') }} minute'
    )
),
distinct_operation_types AS (
    SELECT DISTINCT operation_type
    FROM {{ ref('operation_data') }}
    WHERE operation_type IS NOT NULL
),
bucket_operation_counts AS (
    SELECT
        gs.time AS time_bucket,
        dot.operation_type,
        COUNT(CASE
            WHEN od.operation_status = 'completed' THEN 1
            ELSE NULL
        END) AS completed_count,
        COUNT(CASE
            WHEN od.operation_status = 'failed' THEN 1
            ELSE NULL
        END) AS failed_count
    FROM generate_series gs
    LEFT JOIN {{ ref('operation_data') }} od ON od.timestamp >= gs.time AND od.timestamp < gs.time + INTERVAL '{{ var('bucket_size') }} minute'
    CROSS JOIN distinct_operation_types dot
    GROUP BY gs.time, dot.operation_type
)
SELECT *
FROM bucket_operation_counts
