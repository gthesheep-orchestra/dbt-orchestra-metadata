{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model aggregates task run operations by unique task ID, providing a summary of task names, integrations, average run durations, and counts of failed and skipped runs.
*/

WITH grouped_task_run_or_operation AS (
    SELECT
        tro.unique_task_id,
        tro.task_id,
        tro.pipeline_id,
        (SELECT task_name FROM {{ ref('task_run_or_operation') }} tro2 WHERE tro.unique_task_id = tro2.unique_task_id ORDER BY tro2.updated_at LIMIT 1) AS task_name,
        (SELECT integration FROM {{ ref('task_run_or_operation') }} tro3 WHERE tro.unique_task_id = tro3.unique_task_id ORDER BY tro3.updated_at LIMIT 1) AS integration,
        AVG(run_duration) AS average_duration,
        COUNT(CASE WHEN status = 'FAILED' THEN 1 ELSE NULL END) AS failed_runs,
        COUNT(CASE WHEN status = 'SKIPPED' THEN 1 ELSE NULL END) AS skipped_runs
    FROM {{ ref('task_run_or_operation') }} tro
    GROUP BY unique_task_id, task_id, pipeline_id
)
SELECT *
FROM grouped_task_run_or_operation
ORDER BY {{ sort_column }} {{ sort_direction }} NULLS LAST
LIMIT {{ page_size }} OFFSET {{ offset }};
