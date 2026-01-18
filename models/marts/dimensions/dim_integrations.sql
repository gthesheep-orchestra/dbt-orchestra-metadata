{{
    config(
        materialized='table',
        unique_key='integration_key'
    )
}}

with task_integrations as (

    select distinct
        integration,
        integration_job
    from {{ ref('stg_orchestra__task_runs') }}
    where integration is not null

),

operation_integrations as (

    select distinct
        integration,
        integration_job
    from {{ ref('stg_orchestra__operations') }}
    where integration is not null

),

asset_integrations as (

    select distinct
        integration,
        cast(null as {{ dbt.type_string() }}) as integration_job
    from {{ ref('stg_orchestra__assets') }}
    where integration is not null

),

all_integrations as (

    select * from task_integrations
    union
    select * from operation_integrations
    union
    select * from asset_integrations

),

integration_stats as (

    select
        i.integration,
        i.integration_job,

        -- task stats
        count(distinct t.task_run_id) as total_task_runs,
        sum(case when t.is_successful then 1 else 0 end) as successful_task_runs,
        sum(case when t.is_failed then 1 else 0 end) as failed_task_runs,
        avg(t.duration_seconds) as avg_task_duration_seconds,

        -- operation stats
        count(distinct o.operation_id) as total_operations,
        sum(o.rows_affected) as total_rows_affected,
        avg(o.duration_seconds) as avg_operation_duration_seconds

    from all_integrations i
    left join {{ ref('stg_orchestra__task_runs') }} t
        on i.integration = t.integration
        and (i.integration_job = t.integration_job or (i.integration_job is null and t.integration_job is null))
    left join {{ ref('stg_orchestra__operations') }} o
        on i.integration = o.integration
        and (i.integration_job = o.integration_job or (i.integration_job is null and o.integration_job is null))
    group by 1, 2

)

select
    {{ dbt_utils.generate_surrogate_key(['integration', 'integration_job']) }} as integration_key,
    integration,
    integration_job,
    total_task_runs,
    successful_task_runs,
    failed_task_runs,
    case
        when total_task_runs > 0
        then successful_task_runs * 100.0 / total_task_runs
    end as task_success_rate_pct,
    avg_task_duration_seconds,
    total_operations,
    total_rows_affected,
    avg_operation_duration_seconds,
    current_timestamp as updated_at

from integration_stats
