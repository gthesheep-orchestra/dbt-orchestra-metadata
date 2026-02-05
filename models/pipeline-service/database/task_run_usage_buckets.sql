{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model calculates the total number of task runs, total task run minutes, and total compute minutes for a specified account within defined time buckets.
*/

WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        generate_series + INTERVAL ':bucket_size minute' AS time_to,
        COUNT(task_runs.id) AS total_task_runs,
        SUM(EXTRACT(EPOCH FROM (task_runs.completed_at - task_runs.started_at)) / 60) AS total_task_run_minutes,
        SUM(task_runs.compute_minutes) AS total_compute_minutes
    FROM
        task_runs
    WHERE
        task_runs.account_id = {{ account_id }}
        AND task_runs.completed_at >= {{ time_from }}
        AND task_runs.completed_at <= {{ time_to }}
        AND task_runs.matrix_parent = FALSE
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
