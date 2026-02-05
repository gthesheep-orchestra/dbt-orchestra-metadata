{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model generates a time series of task runs filtered by various parameters such as account ID, pipeline IDs, statuses, and product IDs. It joins the generated time series with the task runs data to provide a comprehensive view of task completions over specified time intervals.
*/

WITH generate_series AS (
    SELECT generate_series(
        {{ var('time_from') }},
        {{ var('time_to') }},
        interval '{{ var('bucket_size') }} minute'
    ) AS time_bucket
),

task_runs_filtered AS (
    SELECT *
    FROM {{ ref('task_runs') }} AS tr
    JOIN {{ ref('pipelines') }} AS p ON p.pipeline_id = tr.pipeline_id
    WHERE p.is_deleted = FALSE
      AND tr.account_id = {{ var('account_id') }}
      AND tr.completed_at >= {{ var('time_from') }}
      AND tr.completed_at <= {{ var('time_to') }}
      AND tr.matrix_parent = FALSE
      AND ({{ var('pipeline_ids') }} IS NULL OR tr.pipeline_id = ANY({{ var('pipeline_ids') }}))
      AND ({{ var('statuses') }} IS NULL OR tr.status = ANY({{ var('statuses') }}))
      AND ({{ var('product_ids') }} IS NULL AND {{ var('include_productless_pipelines') }} = FALSE
           OR {{ var('include_productless_pipelines') }} = TRUE AND p.product_id IS NULL
           OR p.product_id = ANY({{ var('product_ids') }}))
)

SELECT gs.time_bucket,
       tr.*
FROM generate_series gs
LEFT JOIN task_runs_filtered tr ON tr.completed_at >= gs.time_bucket AND tr.completed_at < gs.time_bucket + INTERVAL '1 minute'
