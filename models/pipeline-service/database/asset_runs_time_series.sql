{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/asset_runs.py
  Description: This model generates a time series of asset runs filtered by account ID and integration, allowing for analysis of asset run activity over specified time buckets.
*/


WITH generate_series AS (
    SELECT generate_series(
        {{ var('time_from') }},
        {{ var('time_to') }},
        interval '{{ var('bucket_size') }} minute'
    ) AS time_bucket
),
asset_runs_filtered AS (
    SELECT *
    FROM {{ ref('asset_runs') }}
    WHERE account_id = {{ var('account_id') }}
    AND updated_at >= {{ var('time_from') }}
    AND updated_at <= {{ var('time_to') }}
    AND ({{ var('integration') }} IS NULL OR integration = {{ var('integration') }})
)
SELECT 
    gs.time_bucket,
    ar.*
FROM 
    generate_series gs
LEFT JOIN 
    asset_runs_filtered ar ON ar.updated_at >= gs.time_bucket AND ar.updated_at < gs.time_bucket + INTERVAL '{{ var('bucket_size') }} minute'

