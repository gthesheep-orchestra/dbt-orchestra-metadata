with source as (

    select * from {{ ref('stg_orchestra__task_runs') }}

),

final as (

    select
        task_run_id,
        pipeline_run_id,
        task_name,
        task_status,
        integration,
        integration_job,
        status_message,
        external_status,
        created_at_utc,
        started_at_utc,
        updated_at_utc,
        completed_at_utc,
        duration_seconds,
        is_successful,
        is_failed,
        is_in_progress,
        is_cancelled,
        is_skipped,
        has_warning,
        _dlt_load_id,
        _dlt_id
    from source

)

select * from final
