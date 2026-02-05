{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model filters operations and their associated assets based on various optional parameters, allowing for flexible querying of operation records.
*/

WITH filtered_operations AS (
    SELECT *
    FROM {{ ref('operations') }}
    WHERE account_id = {{ account_id }}
    AND operation_type != 'TEST_GROUP'
    AND ({{ integration }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE integration = {{ integration }}
    ))
    AND ({{ integration_account_id }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE integration_account_id = {{ integration_account_id }}
    ))
    AND ({{ workspace_id }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE workspace_id = {{ workspace_id }}
    ))
    AND ({{ database_name }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE database_name = {{ database_name }}
    ))
    AND ({{ schema_name }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE schema_name = {{ schema_name }}
    ))
    AND ({{ asset_type }} IS NULL OR asset_external_id IN (
        SELECT external_id
        FROM {{ ref('assets') }}
        WHERE asset_type = {{ asset_type }}
    ))
    AND ({{ time_to }} IS NULL OR inserted_at <= {{ time_to }})
    AND ({{ time_from }} IS NULL OR inserted_at >= {{ time_from }})
),
all_buckets AS (
    SELECT
        generate_series AS time_from,
        LEAST(
            {{ time_to }},
            generate_series + INTERVAL '1 day'
        ) AS time_to
    FROM generate_series({{ time_from }}, {{ time_to }}, '1 day')
)
SELECT * FROM filtered_operations;
