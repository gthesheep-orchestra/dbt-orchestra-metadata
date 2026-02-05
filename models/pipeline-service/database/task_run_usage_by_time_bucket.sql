{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model calculates the usage of task runs over specified time buckets, providing metrics such as total task runs, total task run minutes, and total compute minutes for different integrations.
*/


WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        LEAST({{ var('time_to') }}, generate_series + INTERVAL '{{ var('bucket_size') }} minute' - INTERVAL '1 microseconds') AS time_to,
        COUNT(CASE WHEN task_runs.status != 'SKIPPED' THEN task_runs.task_run_id END) AS total_task_runs,
        SUM(
            CASE
                WHEN task_runs.status != 'SKIPPED'
                AND task_runs.started_at IS NOT NULL
                AND task_runs.completed_at IS NOT NULL
                THEN EXTRACT(EPOCH FROM (task_runs.completed_at - task_runs.started_at)) / 60.0
                ELSE 0
            END
        ) AS total_task_run_minutes,
        SUM(
            CASE
                WHEN task_runs.status != 'SKIPPED'
                AND task_runs.started_at IS NOT NULL
                AND task_runs.completed_at IS NOT NULL
                AND task_runs.integration IN ('DBT_CORE', 'PYTHON')
                THEN EXTRACT(EPOCH FROM (task_runs.completed_at - task_runs.started_at)) / 60.0
                ELSE 0
            END
        ) AS total_compute_minutes
    FROM
        GENERATE_SERIES (
            {{ var('time_from') }},
            {{ var('time_to') }},
            interval '{{ var('bucket_size') }} minute'
        ) AS generate_series
    LEFT JOIN (
        SELECT * FROM {{ ref('task_runs') }}
        JOIN {{ ref('pipelines') }} ON {{ ref('pipelines') }}.pipeline_id = {{ ref('task_runs') }}.pipeline_id
        WHERE {{ ref('pipelines') }}.is_deleted = FALSE
        AND {{ ref('task_runs') }}.account_id = {{ var('account_id') }}
        AND {{ ref('task_runs') }}.completed_at >= {{ var('time_from') }}
        AND {{ ref('task_runs') }}.completed_at <= {{ var('time_to') }}
        AND {{ ref('task_runs') }}.matrix_parent = FALSE
    ) AS task_runs ON task_runs.completed_at >= generate_series AND task_runs.completed_at < generate_series + INTERVAL '{{ var('bucket_size') }} minute'
    GROUP BY
        time_from, time_to
)
SELECT
    time_from,
    time_to,
    COALESCE(total_task_runs, 0) AS total_task_runs,
    COALESCE(total_task_run_minutes, 0.0) AS total_task_run_minutes,
    COALESCE(total_compute_minutes, 0.0) AS total_compute_minutes
FROM
    bucket_counts
ORDER BY
    time_from;

