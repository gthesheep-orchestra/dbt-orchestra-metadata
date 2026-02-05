{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/pipeline_runs.py
  Description: This model aggregates pipeline runs by specified time buckets, filtering based on account, pipeline IDs, statuses, and product IDs.
*/

WITH generate_series AS (
    SELECT generate_series(
        {{ var('time_from') }},
        {{ var('time_to') }},
        INTERVAL '{{ var('bucket_size') }} minute'
    ) AS time_bucket
), filtered_pipeline_runs AS (
    SELECT
        pipeline_runs.*
    FROM
        {{ ref('pipeline_runs') }} AS pipeline_runs
    JOIN
        {{ ref('pipelines') }} AS pipelines
    ON
        pipelines.pipeline_id = pipeline_runs.pipeline_id
    WHERE
        pipelines.is_deleted = FALSE
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
)
SELECT
    gs.time_bucket,
    COUNT(fpr.pipeline_id) AS pipeline_run_count
FROM
    generate_series gs
LEFT JOIN
    filtered_pipeline_runs fpr
ON
    COALESCE(fpr.completed_at, fpr.created_at) >= gs.time_bucket
    AND COALESCE(fpr.completed_at, fpr.created_at) < gs.time_bucket + INTERVAL '{{ var('bucket_size') }} minute'
GROUP BY
    gs.time_bucket
ORDER BY
    gs.time_bucket;
