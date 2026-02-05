{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/assets.py
  Description: This model calculates the total number of distinct assets tested, filtering out null external IDs, and generates a time series for the testing period.
*/

WITH total_assets_tested AS (
    SELECT COUNT(DISTINCT external_id) FILTER (WHERE external_id IS NOT NULL) AS total_count
    FROM {{ ref('asset_metrics') }}
    WHERE operation_type = 'TEST'
)
SELECT
    generate_series AS time_from,
    generate_series + INTERVAL '1 day' AS time_to
FROM generate_series(
    (SELECT MIN(time) FROM {{ ref('asset_metrics') }})::timestamp,
    (SELECT MAX(time) FROM {{ ref('asset_metrics') }})::timestamp,
    '1 day'::interval
) AS generate_series;
