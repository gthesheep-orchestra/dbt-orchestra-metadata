{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/asset_runs.py
  Description: This model aggregates asset run analytics by time buckets, counting succeeded and failed asset runs within specified time intervals.
*/

WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        LEAST({{ var('time_to') }}, generate_series + INTERVAL '{{ var('bucket_size') }} minute' - INTERVAL '1 microseconds') AS time_to,
        COUNT(CASE WHEN asset_runs.status = 'SUCCEEDED' THEN asset_runs.asset_run_id END) AS succeeded_count,
        COUNT(CASE WHEN asset_runs.status = 'FAILED' THEN asset_runs.asset_run_id END) AS failed_count
    FROM
        GENERATE_SERIES (
            {{ var('time_from') }},
            {{ var('time_to') }},
            interval '{{ var('bucket_size') }} minute'
        ) AS generate_series
    LEFT JOIN (
        SELECT * FROM {{ ref('asset_runs') }}
        WHERE account_id = {{ var('account_id') }}
        AND updated_at >= {{ var('time_from') }}
        AND updated_at <= {{ var('time_to') }}
        AND ({{ var('integration') }} IS NULL OR integration = {{ var('integration') }})
    ) AS asset_runs ON asset_runs.updated_at >= generate_series AND asset_runs.updated_at < generate_series + INTERVAL '{{ var('bucket_size') }} minute'
    GROUP BY
        time_from, time_to
)
SELECT
    time_from,
    time_to,
    succeeded_count,
    failed_count,
    (SELECT SUM(succeeded_count) FROM bucket_counts) AS total_succeeded,
    (SELECT SUM(failed_count) FROM bucket_counts) AS total_failed
FROM
    bucket_counts
ORDER BY
    time_from;
