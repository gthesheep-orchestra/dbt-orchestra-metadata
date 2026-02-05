{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates operations data, filtering by various parameters and calculating average run durations and counts of completed operations.
*/

WITH filtered_operations AS (
    SELECT
        operations.unique_operation_id,
        operations.integration,
        operations.operation_status,
        COALESCE(operations.completed_at, operations.inserted_at) AS completed_at,
        task_runs.unique_task_id,
        run_duration
    FROM
        {{ ref('operations') }} AS operations
    LEFT JOIN
        {{ ref('task_runs') }} AS task_runs ON operations.unique_operation_id = task_runs.unique_operation_id
    WHERE
        operations.account_id = {{ account_id }}
        AND operations.integration = ANY({{ integrations }})
        AND operations.operation_type <> 'TEST_GROUP'
        AND (:statuses IS NULL OR operations.operation_status = ANY(:statuses))
        AND (:unique_task_ids IS NULL OR task_runs.unique_task_id = ANY(:unique_task_ids))
        AND (:time_from IS NULL OR COALESCE(operations.completed_at, operations.inserted_at) >= :time_from)
        AND (:time_to IS NULL OR COALESCE(operations.completed_at, operations.inserted_at) <= :time_to)
),
grouped_operation AS (
    SELECT
        unique_operation_id,
        integration,
        (SELECT operation_name FROM {{ ref('operation_metrics') }} WHERE unique_operation_id = filtered_operations.unique_operation_id ORDER BY inserted_at LIMIT 1) AS operation_name,
        (SELECT operation_type FROM {{ ref('operation_metrics') }} WHERE unique_operation_id = filtered_operations.unique_operation_id LIMIT 1) AS operation_type,
        AVG(run_duration) AS average_duration,
        COUNT(CASE WHEN operation_status = 'COMPLETED' THEN 1 END) AS completed_count
    FROM
        filtered_operations
    GROUP BY
        unique_operation_id,
        integration
)
SELECT * FROM grouped_operation;
