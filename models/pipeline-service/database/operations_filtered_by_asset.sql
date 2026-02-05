{{
  config(
    materialized='incremental'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model filters operations based on account ID and asset external ID, ensuring that only relevant operations are included for the specified account and asset.
*/

WITH asset_ids AS (
    SELECT external_id
    FROM {{ ref('assets') }}
    WHERE account_id = {{ account_id }}
    AND asset_id = {{ asset_id }}
)
SELECT *
FROM {{ ref('operations') }}
WHERE inserted_at >= generate_series
    AND inserted_at < generate_series + INTERVAL '{{ bucket_size }} minute'
    AND account_id = {{ account_id }}
    AND asset_external_id IN (SELECT external_id FROM asset_ids)
    AND operation_type = {{ operation_type }}
