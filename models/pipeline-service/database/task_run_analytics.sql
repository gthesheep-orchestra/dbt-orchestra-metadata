{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/task_runs.py
  Description: This model aggregates task run analytics by time buckets, counting the number of succeeded, failed, and warning task runs, and provides totals for each category.
*/


WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        LEAST(:time_to, generate_series + INTERVAL ':bucket_size minute') AS time_to,
        COUNT(CASE WHEN task_runs.status = 'succeeded' THEN 1 END) AS succeeded_count,
        COUNT(CASE WHEN task_runs.status = 'failed' THEN 1 END) AS failed_count,
        COUNT(CASE WHEN task_runs.status = 'warning' THEN 1 END) AS warning_count
    FROM
        {{ ref('task_runs') }} AS task_runs
    JOIN
        {{ ref('pipelines') }} AS pipelines ON task_runs.pipeline_id = pipelines.id
    WHERE
        task_runs.completed_at >= generate_series
        AND task_runs.completed_at < generate_series + INTERVAL ':bucket_size minute'
        AND (:statuses IS NULL OR task_runs.status = ANY(:statuses))
        AND (
            (:product_ids IS NULL AND :include_productless_pipelines = FALSE)
            OR (:include_productless_pipelines = TRUE AND pipelines.product_id IS NULL)
            OR pipelines.product_id = ANY(:product_ids)
        )
    GROUP BY
        time_from, time_to
)
SELECT
    time_from,
    time_to,
    succeeded_count,
    failed_count,
    warning_count,
    (SELECT SUM(succeeded_count) FROM bucket_counts) AS total_succeeded,
    (SELECT SUM(failed_count) FROM bucket_counts) AS total_failed,
    (SELECT SUM(warning_count) FROM bucket_counts) AS total_warning
FROM
    bucket_counts
ORDER BY
    time_from;

