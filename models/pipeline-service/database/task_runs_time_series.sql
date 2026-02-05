{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model generates a time series of task runs within a specified time range, grouping them by defined bucket sizes. It joins the task runs with the pipelines to filter out deleted pipelines and includes only relevant task runs based on account ID and completion time.
*/

WITH generate_series AS (
    SELECT generate_series(
        {{ var('time_from') }},
        {{ var('time_to') }},
        interval '{{ var('bucket_size') }} minute'
    ) AS time_bucket
),

task_runs AS (
    SELECT *
    FROM {{ ref('task_runs') }}
    JOIN {{ ref('pipelines') }} ON pipelines.pipeline_id = task_runs.pipeline_id
    WHERE pipelines.is_deleted = FALSE
    AND task_runs.account_id = {{ var('account_id') }}
    AND task_runs.completed_at >= {{ var('time_from') }}
    AND task_runs.completed_at <= {{ var('time_to') }}
    AND task_runs.matrix_parent = FALSE
)

SELECT
    gs.time_bucket,
    tr.*
FROM
    generate_series gs
LEFT JOIN task_runs tr ON tr.completed_at >= gs.time_bucket AND tr.completed_at < gs.time_bucket + INTERVAL '{{ var('bucket_size') }} minute'
