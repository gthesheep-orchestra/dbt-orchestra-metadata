{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model aggregates task run statistics, calculating average run duration, and counting failed and skipped runs for each unique task, filtered by various parameters.
*/


WITH task_run_or_operation AS (
    SELECT
        task_runs.account_id,
        task_runs.unique_task_id,
        task_runs.task_id,
        task_runs.pipeline_id,
        task_runs.task_name,
        task_runs.integration,
        CASE
            WHEN task_runs.completed_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM task_runs.completed_at - task_runs.started_at)
            ELSE NULL
        END AS run_duration,
        task_runs.status,
        task_runs.updated_at
    FROM {{ ref('task_runs') }} AS task_runs
    INNER JOIN {{ ref('pipelines') }} AS pipelines
        ON task_runs.pipeline_id = pipelines.pipeline_id
        AND pipelines.is_deleted = FALSE
        AND (:product_ids IS NULL AND :include_productless_pipelines IS FALSE
            OR :include_productless_pipelines IS TRUE AND pipelines.product_id IS NULL
            OR pipelines.product_id = ANY(:product_ids))
        AND (:pipeline_ids IS NULL OR pipelines.pipeline_id = ANY(:pipeline_ids))
    WHERE task_runs.account_id = :account_id
    AND task_runs.matrix_parent = FALSE
    AND (:time_from IS NULL OR task_runs.updated_at >= :time_from)
    AND (:time_to IS NULL OR task_runs.updated_at <= :time_to)
    AND (:statuses IS NULL OR task_runs.status = ANY(:statuses))
    AND (:unique_task_ids IS NULL OR task_runs.unique_task_id = ANY(:unique_task_ids))
    AND (:integrations IS NULL OR task_runs.integration = ANY(:integrations))
),
grouped_task_run_or_operation AS (
    SELECT
        unique_task_id,
        task_id,
        pipeline_id,
        (SELECT task_name FROM task_run_or_operation WHERE tro.unique_task_id = task_run_or_operation.unique_task_id ORDER BY task_run_or_operation.updated_at LIMIT 1) AS task_name,
        (SELECT integration FROM task_run_or_operation WHERE tro.unique_task_id = task_run_or_operation.unique_task_id ORDER BY task_run_or_operation.updated_at LIMIT 1) AS integration,
        AVG(run_duration) AS average_duration,
        COUNT(CASE WHEN status = 'FAILED' THEN 1 ELSE NULL END) AS failed_runs,
        COUNT(CASE WHEN status = 'SKIPPED' THEN 1 ELSE NULL END) AS skipped_runs
    FROM task_run_or_operation tro
    GROUP BY unique_task_id, task_id, pipeline_id
)
SELECT *
FROM grouped_task_run_or_operation
ORDER BY {{ order_by_column }} {{ order_direction }} NULLS LAST
LIMIT {{ page_size }} OFFSET {{ offset }};
