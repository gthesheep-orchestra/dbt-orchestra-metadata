{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates test operation results for assets by time intervals, providing counts of succeeded, warning, and failed operations along with total counts.
*/


    WITH bucket_counts AS (
        SELECT
            time_from,
            time_to,
            COUNT(CASE WHEN status = 'succeeded' THEN 1 END) AS succeeded_count,
            COUNT(CASE WHEN status = 'warning' THEN 1 END) AS warning_count,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_count
        FROM
            {{ ref('operations') }} AS operations
        INNER JOIN
            {{ ref('assets') }} AS assets ON operations.asset_external_id = assets.external_id
            AND assets.account_id = operations.account_id
        WHERE
            operations.account_id = {{ account_id }}
            AND operations.operation_type = 'TEST'
            AND operations.timestamp BETWEEN {{ time_from }} AND {{ time_to }}
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

