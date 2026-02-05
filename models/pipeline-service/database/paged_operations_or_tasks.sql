{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model retrieves paginated operations or task runs for a specific account, applying various filters for time, status, unique task IDs, and integrations.
*/


WITH task_run_or_operation AS (
    SELECT *
    FROM {{ ref('task_runs') }} AS task_runs
    LEFT JOIN {{ ref('operations') }} AS operations
        ON task_runs.pipeline_run_id = operations.pipeline_run_id
        AND task_runs.task_run_id = operations.task_run_id
    WHERE task_runs.account_id = {{ account_id }}
        AND task_runs.matrix_parent = FALSE
        AND ({{ time_from }} IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) >= {{ time_from }})
        AND ({{ time_to }} IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) <= {{ time_to }})
        AND ({{ statuses }} IS NULL OR COALESCE(operations.operation_status, task_runs.status) = ANY({{ statuses }}))
        AND ({{ unique_task_ids }} IS NULL OR task_runs.unique_task_id = ANY({{ unique_task_ids }}))
        AND ({{ integrations }} IS NULL OR COALESCE(operations.integration, task_runs.integration) = ANY({{ integrations }}))
),
total_count AS (
    SELECT COUNT(*) AS total_rows
    FROM task_run_or_operation
)
SELECT *,
    (SELECT total_rows FROM total_count) AS total_rows
FROM task_run_or_operation
ORDER BY {{ sort_column }} {{ sort_direction }} NULLS LAST
LIMIT {{ page_size }} OFFSET {{ offset }};

