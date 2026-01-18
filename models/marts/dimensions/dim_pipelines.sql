{{
    config(
        materialized='table',
        unique_key='pipeline_id'
    )
}}

with pipeline_runs as (

    select * from {{ ref('stg_orchestra__pipeline_runs') }}

),

pipeline_stats as (

    select
        pipeline_id,
        pipeline_name,

        -- run counts
        count(*) as total_runs,
        sum(case when is_successful then 1 else 0 end) as successful_runs,
        sum(case when is_failed then 1 else 0 end) as failed_runs,
        sum(case when has_warning then 1 else 0 end) as warning_runs,

        -- timing stats
        min(created_at_utc) as first_run_at,
        max(created_at_utc) as last_run_at,
        avg(duration_seconds) as avg_duration_seconds,
        max(duration_seconds) as max_duration_seconds,
        min(duration_seconds) as min_duration_seconds,

        -- success rate
        case
            when count(*) > 0
            then sum(case when is_successful then 1 else 0 end) * 100.0 / count(*)
        end as success_rate_pct,

        -- most common trigger
        mode() within group (order by triggered_by) as most_common_trigger,

        -- most common branch
        mode() within group (order by git_branch) as most_common_branch

    from pipeline_runs
    where not is_in_progress
    group by 1, 2

)

select
    pipeline_id,
    pipeline_name,
    total_runs,
    successful_runs,
    failed_runs,
    warning_runs,
    first_run_at,
    last_run_at,
    avg_duration_seconds,
    max_duration_seconds,
    min_duration_seconds,
    success_rate_pct,
    most_common_trigger,
    most_common_branch,
    current_timestamp as updated_at

from pipeline_stats
