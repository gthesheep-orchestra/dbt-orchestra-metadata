{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates test operation results by time buckets, counting the number of succeeded, warning, and failed operations for specified time intervals.
*/


WITH
    test_operation AS (
        SELECT *
        FROM {{ ref('operations') }} AS operations
        INNER JOIN {{ ref('assets') }} AS assets
        ON operations.asset_external_id = assets.external_id
        AND assets.account_id = operations.account_id
        WHERE operations.account_id = {{ account_id }}
        AND operations.operation_type = 'TEST'
        AND ({{ integration }} IS NULL OR assets.integration = {{ integration }})
        AND ({{ integration_account_id }} IS NULL OR assets.integration_account_id = {{ integration_account_id }})
        AND ({{ workspace_id }} IS NULL OR assets.workspace_id = {{ workspace_id }})
        AND ({{ database_name }} IS NULL OR assets.database_name = {{ database_name }})
        AND ({{ schema_name }} IS NULL OR assets.schema_name = {{ schema_name }})
        AND ({{ asset_type }} IS NULL OR assets.asset_type = {{ asset_type }})
        AND ({{ time_to }} IS NULL OR operations.inserted_at <= {{ time_to }})
        AND ({{ time_from }} IS NULL OR operations.inserted_at >= {{ time_from }})
    ),
    bucket_counts AS (
        SELECT
            generate_series AS time_from,
            LEAST(
                {{ time_to }},
                generate_series + INTERVAL '{{ bucket_size }} minute' - INTERVAL '1 microseconds'
            ) AS time_to,
            COUNT(CASE WHEN test_operation.operation_status = 'SUCCEEDED' THEN test_operation.operation_id END) AS succeeded_count,
            COUNT(CASE WHEN test_operation.operation_status = 'WARNING' THEN test_operation.operation_id END) AS warning_count,
            COUNT(CASE WHEN test_operation.operation_status = 'FAILED' THEN test_operation.operation_id END) AS failed_count
        FROM
            GENERATE_SERIES(
                {{ time_from }},
                {{ time_to }},
                INTERVAL '{{ bucket_size }} minute'
            ) AS generate_series
            LEFT JOIN test_operation ON test_operation.inserted_at >= generate_series
            AND test_operation.inserted_at < generate_series + INTERVAL '{{ bucket_size }} minute'
        GROUP BY
            time_from,
            time_to
    )
SELECT
    time_from,
    time_to,
    succeeded_count,
    warning_count,
    failed_count,
    (SELECT SUM(succeeded_count) FROM bucket_counts) AS total_succeeded,
    (SELECT SUM(warning_count) FROM bucket_counts) AS total_warning,
    (SELECT SUM(failed_count) FROM bucket_counts) AS total_failed
FROM
    bucket_counts
ORDER BY
    time_from;

