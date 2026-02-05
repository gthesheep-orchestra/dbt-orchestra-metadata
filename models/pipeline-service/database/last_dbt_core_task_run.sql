{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model retrieves the most recent dbt core task run for a specified account, pipeline, and task, filtering by project directory and branch parameters.
*/

WITH filtered_task_runs AS (
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
      AND tr.status NOT IN ('failed', 'canceled')
      AND (
          tr.task_parameters->>'project_dir' = {{ project_dir }}
          OR {{ project_dir }} IS NULL
      )
      AND (
          ({{ dbt_branch }} IS NULL AND tr.task_parameters->>'branch' IS NULL)
          OR tr.task_parameters->>'branch' = {{ dbt_branch }}
      )
)
SELECT *
FROM filtered_task_runs
ORDER BY completed_at DESC
LIMIT 1;
