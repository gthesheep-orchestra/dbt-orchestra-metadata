{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model retrieves the last successful task run ID for a specific account, pipeline, and task, ensuring that the task run is associated with the default branch of the pipeline.
*/

WITH last_successful_task_run AS (
    SELECT tr.task_run_id
    FROM {{ ref('task_runs') }} tr
    JOIN {{ ref('pipelines') }} p ON tr.pipeline_id = p.pipeline_id
    JOIN {{ ref('production_runs') }} pr ON tr.task_id = pr.task_id
    WHERE
        tr.account_id = {{ account_id }}
        AND tr.pipeline_id = {{ pipeline_id }}
        AND tr.task_id = {{ task_id }}
        AND tr.status = 'SUCCEEDED'
        AND pr.branch IS NOT NULL
        AND pr.branch = p.default_branch
    ORDER BY
        tr.completed_at DESC
    LIMIT 1
)
SELECT * FROM last_successful_task_run;
