{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/assets.py
  Description: This model calculates the coverage of assets managed by the system, including the total number of assets and the number of assets that have been tested within a specified time range.
*/


WITH asset_metrics AS (
    SELECT
        assets.external_id,
        operations.external_id AS operation_external_id,
        operations.operation_type,
        operations.inserted_at
    FROM {{ ref('assets') }} AS assets
    INNER JOIN {{ ref('operations') }} AS operations
        ON assets.external_id = operations.asset_external_id
        AND operations.account_id = {{ account_id }}
    WHERE assets.account_id = {{ account_id }}
    AND operations.inserted_at >= {{ time_from }}
    AND operations.inserted_at <= {{ time_to }}
    AND ({{ integration }} IS NULL OR assets.integration = {{ integration }})
    AND ({{ integration_account_id }} IS NULL OR assets.integration_account_id = {{ integration_account_id }})
    AND ({{ workspace_id }} IS NULL OR assets.workspace_id = {{ workspace_id }})
    AND ({{ database_name }} IS NULL OR assets.database_name = {{ database_name }})
    AND ({{ schema_name }} IS NULL OR assets.schema_name = {{ schema_name }})
    AND ({{ asset_type }} IS NULL OR assets.asset_type = {{ asset_type }})
),
total_assets_managed_by_orchestra AS (
    SELECT COUNT(DISTINCT external_id) FILTER (WHERE external_id IS NOT NULL) AS total_count
    FROM asset_metrics
),
total_assets_tested AS (
    SELECT COUNT(DISTINCT external_id) FILTER (WHERE external_id IS NOT NULL) AS total_count
    FROM asset_metrics
    WHERE operation_type = 'test'
)
SELECT
    (SELECT total_count FROM total_assets_managed_by_orchestra) AS total_assets_managed,
    (SELECT total_count FROM total_assets_tested) AS total_assets_tested

