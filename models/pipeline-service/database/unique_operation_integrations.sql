{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model retrieves unique operation integrations based on account ID, time filters, and optional pipeline IDs from task runs and operations.
*/

WITH task_run_or_operation AS (
    SELECT
        task_runs.account_id,
        task_runs.pipeline_id,
        operations.completed_at,
        operations.inserted_at,
        task_runs.updated_at,
        operations.operation_type,
        task_runs.pipeline_run_id,
        task_runs.task_run_id
    FROM
        {{ ref('task_runs') }} AS task_runs
    LEFT JOIN
        {{ ref('pipelines') }} AS pipelines
        ON task_runs.pipeline_id = pipelines.pipeline_id
    LEFT JOIN
        {{ ref('operations') }} AS operations
        ON task_runs.pipeline_run_id = operations.pipeline_run_id
        AND task_runs.task_run_id = operations.task_run_id
        AND operations.operation_type <> 'TEST_GROUP'
    WHERE
        task_runs.account_id = {{ account_id }}
        AND task_runs.matrix_parent = FALSE
        AND ({{ time_from }} IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) >= {{ time_from }})
        AND ({{ time_to }} IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) <= {{ time_to }})
        AND ({{ pipeline_ids }} IS NULL OR pipelines.pipeline_id = ANY({{ pipeline_ids }}))
)
SELECT DISTINCT
    integration
FROM
    task_run_or_operation;
