{{
    config(
        materialized='table'
    )
}}

-- Sourced from the fact tables rather than staging: they are materialised (not
-- views over the raw dlt source) and already carry integration_key, so this model
-- neither re-scans the source nor re-hashes the surrogate key over every row.
-- It also guarantees these counts agree with fct_task_runs / fct_operations.

with task_stats as (

    select
        integration_key,
        integration,
        integration_job,
        -- grain is one row per task_run_id (tested unique), so count(*) is
        -- equivalent to count(distinct task_run_id) without the shuffle
        count(*) as total_task_runs,
        sum(case when is_successful then 1 else 0 end) as successful_task_runs,
        sum(case when is_failed then 1 else 0 end) as failed_task_runs,
        avg(duration_seconds) as avg_task_duration_seconds
    from {{ ref('fct_task_runs') }}
    where integration is not null
    group by integration_key, integration, integration_job

),

operation_stats as (

    select
        integration_key,
        integration,
        integration_job,
        -- grain is one row per operation_id (tested unique)
        count(*) as total_operations,
        sum(rows_affected) as total_rows_affected,
        avg(duration_seconds) as avg_operation_duration_seconds
    from {{ ref('fct_operations') }}
    where integration is not null
    group by integration_key, integration, integration_job

),

asset_keys as (

    -- assets have no integration_job, so their key is hashed over a null job
    select distinct
        integration,
        cast(null as {{ dbt.type_string() }}) as integration_job
    from {{ ref('dim_assets') }}
    where integration is not null

),

asset_integrations as (

    select
        {{ dbt_utils.generate_surrogate_key(['integration', 'integration_job']) }} as integration_key,
        integration,
        integration_job
    from asset_keys

),

all_integrations as (

    select integration_key, integration, integration_job from task_stats
    union distinct
    select integration_key, integration, integration_job from operation_stats
    union distinct
    select integration_key, integration, integration_job from asset_integrations

)

select
    ai.integration_key,
    ai.integration,
    ai.integration_job,
    coalesce(t.total_task_runs, 0) as total_task_runs,
    coalesce(t.successful_task_runs, 0) as successful_task_runs,
    coalesce(t.failed_task_runs, 0) as failed_task_runs,
    case
        when t.total_task_runs > 0
            then t.successful_task_runs * 100.0 / t.total_task_runs
    end as task_success_rate_pct,
    t.avg_task_duration_seconds,
    coalesce(o.total_operations, 0) as total_operations,
    o.total_rows_affected,
    o.avg_operation_duration_seconds,
    current_timestamp as updated_at

from all_integrations as ai
left join task_stats as t on ai.integration_key = t.integration_key
left join operation_stats as o on ai.integration_key = o.integration_key
