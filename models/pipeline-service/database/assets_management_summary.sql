{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/assets.py
  Description: This model summarizes asset management activities over specified time intervals, counting the number of assets managed and tested by Orchestra.
*/


WITH time_series AS (
    SELECT
        generate_series AS time_from,
        generate_series + INTERVAL '{{ var('bucket_size', 5) }} minute' AS time_to
    FROM
        GENERATE_SERIES('{{ var('time_from') }}', '{{ var('time_to') }}', INTERVAL '1 minute')
)
SELECT
    ts.time_from,
    ts.time_to,
    COUNT(DISTINCT m.external_id) AS assets_managed_by_orchestra,
    COUNT(DISTINCT CASE WHEN m.operation_type = 'TEST' THEN m.external_id END) AS assets_tested,
    (SELECT total_count FROM {{ ref('total_assets_managed_by_orchestra') }}) AS total_assets_managed_by_orchestra,
    (SELECT total_count FROM {{ ref('total_assets_tested') }}) AS total_assets_tested
FROM
    time_series ts
LEFT JOIN
    {{ source('your_source_schema', 'metrics') }} m ON m.timestamp BETWEEN ts.time_from AND ts.time_to
GROUP BY
    ts.time_from, ts.time_to
ORDER BY
    ts.time_from
