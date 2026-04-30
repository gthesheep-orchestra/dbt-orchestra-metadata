{{
    config(
        materialized='incremental',
        unique_key='task_run_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with task_runs as (

    select * from {{ ref('stg_orchestra__task_runs') }}

    {% if is_incremental() %}
    where updated_at_utc > (select max(updated_at_utc) from {{ this }})
    {% endif %}

),

operation_stats as (

    select
        task_run_id,
        count(*) as total_operations,
        sum(case when is_successful then 1 else 0 end) as successful_operations,
        sum(case when is_failed then 1 else 0 end) as failed_operations,
        sum(case when is_skipped then 1 else 0 end) as skipped_operations,
        sum(rows_affected) as total_rows_affected,
        sum(duration_seconds) as total_operation_duration_seconds,

        -- operation type breakdowns
        sum(case when is_ingestion_operation then 1 else 0 end) as ingestion_operations,
        sum(case when is_transformation_operation then 1 else 0 end) as transformation_operations,
        sum(case when is_testing_operation then 1 else 0 end) as testing_operations,
        sum(case when is_deployment_operation then 1 else 0 end) as deployment_operations

    from {{ ref('stg_orchestra__operations') }}

    {% if is_incremental() %}
    where task_run_id in (select task_run_id from task_runs)
    {% endif %}

    group by 1

),

pipeline_context as (

    select
        pipeline_run_id,
        pipeline_id,
        pipeline_name,
        git_branch,
        git_commit_sha

    from {{ ref('int_orchestra__pipeline_run_context') }}

),

final as (

    select
        -- ids
        tr.task_run_id,
        tr.pipeline_run_id,
        pc.pipeline_id,

        -- integration key for joining to dim_integrations
        {{ dbt_utils.generate_surrogate_key(['tr.integration', 'tr.integration_job']) }} as integration_key,

        -- attributes
        tr.task_name,
        tr.task_status,
        tr.integration,
        tr.integration_job,
        tr.status_message,
        tr.external_status,

        -- pipeline context
        pc.pipeline_name,
        pc.git_branch,
        pc.git_commit_sha,

        -- timestamps
        tr.created_at_utc,
        tr.started_at_utc,
        tr.updated_at_utc,
        tr.completed_at_utc,

        -- date keys for dimensional analysis
        cast(tr.created_at_utc as date) as created_date,
        cast(tr.started_at_utc as date) as started_date,
        cast(tr.completed_at_utc as date) as completed_date,

        -- task run metrics
        tr.duration_seconds,
        tr.duration_seconds / 60.0 as duration_minutes,

        -- status flags
        tr.is_successful,
        tr.is_failed,
        tr.is_in_progress,
        tr.is_cancelled,
        tr.is_skipped,
        tr.has_warning,

        -- operation aggregates
        coalesce(os.total_operations, 0) as total_operations,
        coalesce(os.successful_operations, 0) as successful_operations,
        coalesce(os.failed_operations, 0) as failed_operations,
        coalesce(os.skipped_operations, 0) as skipped_operations,
        coalesce(os.total_rows_affected, 0) as total_rows_affected,
        coalesce(os.total_operation_duration_seconds, 0) as total_operation_duration_seconds,

        -- operation type breakdowns
        coalesce(os.ingestion_operations, 0) as ingestion_operations,
        coalesce(os.transformation_operations, 0) as transformation_operations,
        coalesce(os.testing_operations, 0) as testing_operations,
        coalesce(os.deployment_operations, 0) as deployment_operations,

        -- operation success rate
        case
            when coalesce(os.total_operations, 0) > 0
                then coalesce(os.successful_operations, 0) * 100.0 / os.total_operations
        end as operation_success_rate_pct,

        -- dlt metadata
        tr._dlt_load_id,
        tr._dlt_id

    from task_runs as tr
    left join operation_stats as os on tr.task_run_id = os.task_run_id
    left join pipeline_context as pc on tr.pipeline_run_id = pc.pipeline_run_id

)

select * from final
