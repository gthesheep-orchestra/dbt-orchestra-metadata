{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/pipeline_runs.py
  Description: This model aggregates pipeline run analytics over specified time buckets, counting the number of succeeded, failed, and warning pipeline runs while providing total counts across the entire range.
*/


WITH bucket_counts AS (
    SELECT
        generate_series AS time_from,
        LEAST(
            {{ var('time_to') }},
            generate_series + INTERVAL '{{ var('bucket_size') }} minute' - INTERVAL '1 microseconds'
        ) AS time_to,
        COUNT(CASE WHEN pipeline_runs.status = 'SUCCEEDED' THEN pipeline_runs.pipeline_run_id END) AS succeeded_count,
        COUNT(CASE WHEN pipeline_runs.status = 'FAILED' THEN pipeline_runs.pipeline_run_id END) AS failed_count,
        COUNT(CASE WHEN pipeline_runs.status = 'WARNING' THEN pipeline_runs.pipeline_run_id END) AS warning_count
    FROM
        GENERATE_SERIES(
            {{ var('time_from') }},
            {{ var('time_to') }},
            INTERVAL '{{ var('bucket_size') }} minute'
        ) AS generate_series
    LEFT JOIN (
        SELECT pipeline_runs.*
        FROM {{ ref('pipeline_runs') }} AS pipeline_runs
        JOIN {{ ref('pipelines') }} AS pipelines ON pipelines.pipeline_id = pipeline_runs.pipeline_id
        WHERE pipelines.is_deleted = FALSE
            AND pipeline_runs.account_id = {{ var('account_id') }}
            AND COALESCE(pipeline_runs.completed_at, pipeline_runs.created_at) >= {{ var('time_from') }}
            AND COALESCE(pipeline_runs.completed_at, pipeline_runs.created_at) <= {{ var('time_to') }}
            AND ({{ var('pipeline_ids') }} IS NULL OR pipeline_runs.pipeline_id = ANY({{ var('pipeline_ids') }}))
            AND ({{ var('statuses') }} IS NULL OR pipeline_runs.status = ANY({{ var('statuses') }}))
            AND (
                ({{ var('product_ids') }} IS NULL AND {{ var('include_productless_pipelines') }} = FALSE)
                OR ({{ var('include_productless_pipelines') }} = TRUE AND pipelines.product_id IS NULL)
                OR pipelines.product_id = ANY({{ var('product_ids') }})
            )
    ) AS pipeline_runs
    ON COALESCE(pipeline_runs.completed_at, pipeline_runs.created_at) >= generate_series
        AND COALESCE(pipeline_runs.completed_at, pipeline_runs.created_at) < generate_series + INTERVAL '{{ var('bucket_size') }} minute'
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

