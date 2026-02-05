{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model summarizes operation metrics by integration and unique operation ID, calculating average run duration, and counts of failed and skipped runs.
*/


WITH operation_metrics AS (
    SELECT
        CASE
            WHEN operations.integration IN ('DBT', 'DBT_CORE')
            THEN external_id
            ELSE NULL
        END AS unique_operation_id,
        operations.operation_name,
        operations.integration,
        operations.operation_type,
        operations.operation_status,
        operations.inserted_at,
        CASE
            WHEN operations.started_at IS NOT NULL AND operations.completed_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM operations.completed_at - operations.started_at)
            ELSE NULL
        END AS run_duration
    FROM {{ ref('operations') }} AS operations
    INNER JOIN {{ ref('task_runs') }} AS task_runs
        ON task_runs.task_run_id = operations.task_run_id
    INNER JOIN {{ ref('pipelines') }} AS pipelines
        ON task_runs.pipeline_id = pipelines.pipeline_id
        AND pipelines.is_deleted = FALSE
        AND (:product_ids IS NULL AND :include_productless_pipelines IS FALSE
            OR :include_productless_pipelines IS TRUE AND pipelines.product_id IS NULL
            OR pipelines.product_id = ANY(:product_ids))
        AND (:pipeline_ids IS NULL OR pipelines.pipeline_id = ANY(:pipeline_ids))
    WHERE operations.account_id = :account_id
    AND operations.integration = ANY(:integrations)
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
        (SELECT operation_name FROM operation_metrics WHERE om.unique_operation_id = operation_metrics.unique_operation_id ORDER BY operation_metrics.inserted_at LIMIT 1) AS operation_name,
        (SELECT operation_type FROM operation_metrics WHERE om.unique_operation_id = operation_metrics.unique_operation_id LIMIT 1) AS operation_type,
        AVG(run_duration) AS average_duration,
        COUNT(CASE WHEN operation_status = 'FAILED' THEN 1 ELSE NULL END) AS failed_runs,
        COUNT(CASE WHEN operation_status = 'SKIPPED' THEN 1 ELSE NULL END) AS skipped_runs
    FROM operation_metrics om
    GROUP BY integration, unique_operation_id
)
SELECT *
FROM grouped_operation
ORDER BY {{ order_by_column }} {{ order_direction }} NULLS LAST
LIMIT :page_size OFFSET :offset;

