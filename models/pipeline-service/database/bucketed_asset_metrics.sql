{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/asset_metrics_bucketed_hour.py
  Description: This model aggregates asset metrics into hourly buckets based on specified time intervals, allowing for analysis of query counts, costs, and views over time.
*/


WITH hourly_buckets AS (
    SELECT
        start_bucket_date,
        end_bucket_date,
        query_count,
        query_cost,
        query_cost_label,
        query_cost_units,
        view_count,
        partial
    FROM {{ ref('asset_metrics_bucketed_hour') }}
    WHERE account_id = {{ account_id }}
    AND asset_external_id = {{ asset_external_id }}
    AND start_bucket_date >= {{ time_from }}
    AND end_bucket_date <= {{ time_to }}
)
SELECT
    generate_series AS time_from,
    generate_series + INTERVAL '{{ bucket_size }} minute' - INTERVAL '1 microseconds' AS time_to,
    SUM(buckets.query_count) AS query_count,
    SUM(buckets.query_cost) AS query_cost,
    SUM(buckets.view_count) AS view_count,
    (ARRAY_AGG(query_cost_label) FILTER (WHERE query_cost > 0))[1] AS query_cost_label,
    (ARRAY_AGG(query_cost_units) FILTER (WHERE query_cost > 0))[1] AS query_cost_units,
    BOOL_OR(buckets.partial) AS partial
FROM GENERATE_SERIES(
    {{ time_from }},
    {{ time_to }} - INTERVAL '1 microseconds',
    INTERVAL '{{ bucket_size }} minute'
) AS generate_series
LEFT JOIN hourly_buckets AS buckets
    ON buckets.start_bucket_date >= generate_series
    AND buckets.end_bucket_date <= generate_series + INTERVAL '{{ bucket_size }} minute'
GROUP BY time_from, time_to
ORDER BY time_from;

