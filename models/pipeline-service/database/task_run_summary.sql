{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model summarizes task run data, calculating run durations and average durations per unique task, while filtering based on various criteria such as account ID, time range, and pipeline status.
*/

SELECT
    tr.task_run_id,
    tr.pipeline_run_id,
    tr.task_id,
    tr.task_name,
    tr.pipeline_id,
    tr.integration,
    tr.integration_job,
    tr.status,
    tr.message,
    tr.started_at,
    tr.completed_at,
    tr.updated_at,
    CASE
        WHEN tr.started_at IS NOT NULL AND tr.completed_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (tr.completed_at - tr.started_at))
        ELSE NULL
    END AS run_duration_seconds,
    AVG(
        EXTRACT(EPOCH FROM (tr.completed_at - tr.started_at))
    ) FILTER (WHERE tr.started_at IS NOT NULL AND tr.completed_at IS NOT NULL)
      OVER (PARTITION BY tr.unique_task_id) AS avg_duration_seconds,
    CASE
        WHEN tr.started_at IS NOT NULL AND tr.completed_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (tr.completed_at - tr.started_at))
             / NULLIF(
                 AVG(
                     EXTRACT(EPOCH FROM (tr.completed_at - tr.started_at))
                 ) FILTER (WHERE tr.started_at IS NOT NULL AND tr.completed_at IS NOT NULL)
                   OVER (PARTITION BY tr.unique_task_id),
                 0
             )
        ELSE NULL
    END AS duration_ratio
FROM {{ ref('task_runs') }} tr
LEFT JOIN {{ ref('pipelines') }} p ON
    tr.pipeline_id = p.pipeline_id
WHERE tr.account_id = {{ account_id }}
  AND tr.updated_at >= {{ time_from }}
  AND tr.updated_at <= {{ time_to }}
  AND tr.matrix_parent = FALSE
  AND tr.status NOT IN ('CREATED', 'QUEUED', 'RUNNING', 'CANCELLING', 'SKIPPED')
  AND p.is_deleted = FALSE
  AND (
        {{ product_ids }} IS NULL AND {{ include_productless_pipelines }} IS FALSE
        OR {{ include_productless_pipelines }} IS TRUE AND p.product_id IS NULL
        OR p.product_id = ANY({{ product_ids }})
  )
  AND ({{ pipeline_ids }} IS NULL OR tr.pipeline_id = ANY({{ pipeline_ids }}))
ORDER BY {{ order_by }} {{ order_direction }} NULLS LAST
LIMIT {{ limit }};
