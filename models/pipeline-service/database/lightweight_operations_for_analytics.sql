{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model retrieves lightweight operations for analytics by joining task runs with operations and pipelines, applying various filters based on account ID, time range, and other optional parameters.
*/


WITH task_run_or_operation AS (
    SELECT
        task_runs.account_id,
        task_runs.pipeline_run_id,
        task_runs.task_run_id,
        COALESCE(operations.operation_status, task_runs.status) AS operation_status,
        COALESCE(operations.integration, task_runs.integration) AS integration,
        COALESCE(operations.completed_at, operations.inserted_at, task_runs.completed_at) AS completed_at,
        task_runs.started_at,
        task_runs.completed_at,
        task_runs.unique_task_id,
        pipelines.product_id,
        pipelines.pipeline_id
    FROM {{ ref('task_runs') }} AS task_runs
    LEFT JOIN {{ ref('operations') }} AS operations
        ON operations.account_id = task_runs.account_id
        AND task_runs.pipeline_run_id = operations.pipeline_run_id
        AND task_runs.task_run_id = operations.task_run_id
        AND operations.operation_type <> 'TEST_GROUP'
    LEFT JOIN {{ ref('pipelines') }} AS pipelines
        ON task_runs.pipeline_id = pipelines.pipeline_id
    WHERE task_runs.account_id = {{ account_id }}
        AND task_runs.started_at >= {{ time_from }}
        AND task_runs.completed_at < {{ time_to }}
        AND task_runs.matrix_parent = FALSE
        AND pipelines.is_deleted = FALSE
        AND ({{ unique_task_ids }} IS NULL OR task_runs.unique_task_id = ANY({{ unique_task_ids }}))
        AND ({{ product_ids }} IS NULL OR {{ product_ids_include_none }} = TRUE AND pipelines.product_id IS NULL OR pipelines.product_id = ANY({{ product_ids }}))
        AND ({{ pipeline_ids }} IS NULL OR pipelines.pipeline_id = ANY({{ pipeline_ids }}))
        AND ({{ statuses }} IS NULL OR COALESCE(operations.operation_status, task_runs.status) = ANY({{ statuses }}))
        AND ({{ integrations }} IS NULL OR COALESCE(operations.integration, task_runs.integration) = ANY({{ integrations }}))
        AND COALESCE(operations.completed_at, operations.inserted_at, task_runs.completed_at) >= {{ time_from }}
        AND COALESCE(operations.completed_at, operations.inserted_at, task_runs.completed_at) <= {{ time_to }}
)
SELECT * FROM task_run_or_operation;
