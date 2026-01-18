{{
    config(
        materialized='incremental',
        unique_key='summary_date',
        incremental_strategy='merge'
    )
}}

with pipeline_runs as (

    select * from {{ ref('fct_pipeline_runs') }}
    where not is_in_progress

    {% if is_incremental() %}
    and created_date >= (select max(summary_date) - interval '1 day' from {{ this }})
    {% endif %}

),

task_runs as (

    select * from {{ ref('fct_task_runs') }}
    where not is_in_progress

    {% if is_incremental() %}
    and created_date >= (select max(summary_date) - interval '1 day' from {{ this }})
    {% endif %}

),

operations as (

    select * from {{ ref('fct_operations') }}

    {% if is_incremental() %}
    where created_date >= (select max(summary_date) - interval '1 day' from {{ this }})
    {% endif %}

),

pipeline_daily as (

    select
        created_date as summary_date,

        -- pipeline counts
        count(*) as pipeline_runs,
        sum(case when is_successful then 1 else 0 end) as successful_pipeline_runs,
        sum(case when is_failed then 1 else 0 end) as failed_pipeline_runs,
        sum(case when has_warning then 1 else 0 end) as warning_pipeline_runs,
        sum(case when is_cancelled then 1 else 0 end) as cancelled_pipeline_runs,

        -- pipeline timing
        avg(duration_seconds) as avg_pipeline_duration_seconds,
        max(duration_seconds) as max_pipeline_duration_seconds,
        sum(duration_seconds) as total_pipeline_duration_seconds,

        -- unique counts
        count(distinct pipeline_id) as unique_pipelines_run,
        count(distinct triggered_by) as unique_triggers

    from pipeline_runs
    group by 1

),

task_daily as (

    select
        created_date as summary_date,

        -- task counts
        count(*) as task_runs,
        sum(case when is_successful then 1 else 0 end) as successful_task_runs,
        sum(case when is_failed then 1 else 0 end) as failed_task_runs,
        sum(case when is_skipped then 1 else 0 end) as skipped_task_runs,

        -- task timing
        avg(duration_seconds) as avg_task_duration_seconds,
        sum(duration_seconds) as total_task_duration_seconds,

        -- integration counts
        count(distinct integration) as unique_integrations,

        -- operation aggregates from tasks
        sum(total_rows_affected) as total_rows_affected

    from task_runs
    group by 1

),

operations_daily as (

    select
        created_date as summary_date,

        -- operation counts
        count(*) as total_operations,
        sum(case when is_successful then 1 else 0 end) as successful_operations,
        sum(case when is_failed then 1 else 0 end) as failed_operations,

        -- operation types
        sum(case when is_ingestion_operation then 1 else 0 end) as ingestion_operations,
        sum(case when is_transformation_operation then 1 else 0 end) as transformation_operations,
        sum(case when is_testing_operation then 1 else 0 end) as testing_operations,
        sum(case when is_deployment_operation then 1 else 0 end) as deployment_operations,

        -- operation metrics
        sum(rows_affected) as operation_rows_affected,
        avg(duration_seconds) as avg_operation_duration_seconds,
        sum(duration_seconds) as total_operation_duration_seconds

    from operations
    group by 1

),

final as (

    select
        coalesce(pd.summary_date, td.summary_date, od.summary_date) as summary_date,

        -- pipeline metrics
        coalesce(pd.pipeline_runs, 0) as pipeline_runs,
        coalesce(pd.successful_pipeline_runs, 0) as successful_pipeline_runs,
        coalesce(pd.failed_pipeline_runs, 0) as failed_pipeline_runs,
        coalesce(pd.warning_pipeline_runs, 0) as warning_pipeline_runs,
        coalesce(pd.cancelled_pipeline_runs, 0) as cancelled_pipeline_runs,
        case
            when coalesce(pd.pipeline_runs, 0) > 0
            then coalesce(pd.successful_pipeline_runs, 0) * 100.0 / pd.pipeline_runs
        end as pipeline_success_rate_pct,
        pd.avg_pipeline_duration_seconds,
        pd.max_pipeline_duration_seconds,
        coalesce(pd.total_pipeline_duration_seconds, 0) as total_pipeline_duration_seconds,
        coalesce(pd.unique_pipelines_run, 0) as unique_pipelines_run,
        coalesce(pd.unique_triggers, 0) as unique_triggers,

        -- task metrics
        coalesce(td.task_runs, 0) as task_runs,
        coalesce(td.successful_task_runs, 0) as successful_task_runs,
        coalesce(td.failed_task_runs, 0) as failed_task_runs,
        coalesce(td.skipped_task_runs, 0) as skipped_task_runs,
        case
            when coalesce(td.task_runs, 0) > 0
            then coalesce(td.successful_task_runs, 0) * 100.0 / td.task_runs
        end as task_success_rate_pct,
        td.avg_task_duration_seconds,
        coalesce(td.total_task_duration_seconds, 0) as total_task_duration_seconds,
        coalesce(td.unique_integrations, 0) as unique_integrations,
        coalesce(td.total_rows_affected, 0) as total_rows_affected,

        -- operation metrics
        coalesce(od.total_operations, 0) as total_operations,
        coalesce(od.successful_operations, 0) as successful_operations,
        coalesce(od.failed_operations, 0) as failed_operations,
        case
            when coalesce(od.total_operations, 0) > 0
            then coalesce(od.successful_operations, 0) * 100.0 / od.total_operations
        end as operation_success_rate_pct,
        coalesce(od.ingestion_operations, 0) as ingestion_operations,
        coalesce(od.transformation_operations, 0) as transformation_operations,
        coalesce(od.testing_operations, 0) as testing_operations,
        coalesce(od.deployment_operations, 0) as deployment_operations,
        coalesce(od.operation_rows_affected, 0) as operation_rows_affected,
        od.avg_operation_duration_seconds,
        coalesce(od.total_operation_duration_seconds, 0) as total_operation_duration_seconds,

        -- computed fields
        case
            when coalesce(pd.pipeline_runs, 0) > 0
            then coalesce(td.task_runs, 0) * 1.0 / pd.pipeline_runs
        end as avg_tasks_per_pipeline,

        current_timestamp as updated_at

    from pipeline_daily pd
    full outer join task_daily td on pd.summary_date = td.summary_date
    full outer join operations_daily od on coalesce(pd.summary_date, td.summary_date) = od.summary_date

)

select * from final
where summary_date is not null
