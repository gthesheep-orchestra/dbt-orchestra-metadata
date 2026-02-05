{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model aggregates task run and operation data, providing insights into task performance, including durations, statuses, and costs, while allowing for pagination.
*/

WITH task_run_or_operation AS (
    SELECT
        task_runs.account_id,
        task_runs.pipeline_run_id,
        task_runs.task_id,
        CASE
            WHEN operations.operation_id IS NOT NULL
            THEN CONCAT('operation_', operations.operation_id)
            ELSE CONCAT('task_run_', task_runs.task_run_id)
        END AS id,
        CASE
            WHEN operations.operation_id IS NOT NULL
            THEN operations.operation_name
            ELSE task_runs.task_name
        END AS name,
        CASE
            WHEN operations.operation_id IS NOT NULL
            THEN operations.operation_status
            ELSE task_runs.status
        END AS status,
        operations.operation_type AS operation_type,
        CASE
            WHEN operations.operation_id IS NOT NULL
            THEN operations.operation_duration
            ELSE
                CASE
                    WHEN task_runs.completed_at IS NOT NULL
                    THEN EXTRACT(EPOCH FROM task_runs.completed_at - task_runs.started_at)
                    ELSE NULL
                END
        END AS run_duration,
        AVG(
            operations.operation_duration
        ) FILTER (
            WHERE operations.operation_duration IS NOT NULL
            AND operations.integration IN ('DBT', 'DBT_CORE')
        )
        OVER (PARTITION BY operations.integration, operations.external_id) AS avg_duration_seconds,
        CASE
            WHEN operations.operation_duration IS NOT NULL
            AND operations.integration IN ('DBT', 'DBT_CORE')
            THEN operations.operation_duration
                / NULLIF(
                    AVG(operations.operation_duration)
                    FILTER (WHERE operations.operation_duration IS NOT NULL)
                    OVER (PARTITION BY operations.integration, operations.external_id),
                    0
                )
            ELSE NULL
        END AS duration_ratio,
        operations.raw_cost_amount AS raw_cost_amount,
        operations.raw_cost_units AS raw_cost_units,
        operations.raw_cost_integration AS raw_cost_integration,
        CASE
            WHEN operations.operation_id IS NOT NULL
            THEN operations.message
            ELSE task_runs.message
        END AS message,
        task_runs.task_run_id,
        task_runs.task_name,
        COALESCE(operations.integration, task_runs.integration) AS integration,
        COALESCE(operations.integration_job, task_runs.integration_job) AS integration_job,
        task_runs.created_at,
        COALESCE(operations.started_at, task_runs.started_at) AS started_at,
        COALESCE(operations.completed_at, task_runs.completed_at) AS completed_at,
        COALESCE(operations.inserted_at, task_runs.updated_at) AS updated_at
    FROM {{ ref('task_runs') }} AS task_runs
    INNER JOIN {{ ref('pipelines') }} AS pipelines
        ON task_runs.pipeline_id = pipelines.pipeline_id
        AND pipelines.is_deleted = FALSE
        AND (:product_ids_include_none = TRUE AND pipelines.product_id IS NULL
          OR :product_ids IS NULL OR pipelines.product_id = ANY(:product_ids))
        AND (:pipeline_ids IS NULL OR pipelines.pipeline_id = ANY(:pipeline_ids))
    {} JOIN {{ ref('operations') }} AS operations
        ON task_runs.pipeline_run_id = operations.pipeline_run_id
        AND task_runs.task_run_id = operations.task_run_id
        AND operations.operation_type <> 'TEST_GROUP'
    WHERE task_runs.account_id = :account_id
    AND task_runs.matrix_parent = FALSE
    AND (:time_from IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) >= :time_from)
    AND (:time_to IS NULL OR COALESCE(operations.completed_at, operations.inserted_at, task_runs.updated_at) <= :time_to)
    AND (:statuses IS NULL OR COALESCE(operations.operation_status, task_runs.status) = ANY(:statuses))
    AND (:unique_task_ids IS NULL OR task_runs.unique_task_id = ANY(:unique_task_ids))
    AND (:integrations IS NULL OR COALESCE(operations.integration, task_runs.integration) = ANY(:integrations))
),
    total_count AS (
        SELECT COUNT(*) AS total_rows
        FROM task_run_or_operation
    )
    SELECT *,
        (SELECT total_rows FROM total_count) AS total_rows
    FROM task_run_or_operation
    ORDER BY {} {} NULLS LAST
    LIMIT :page_size OFFSET :offset;
