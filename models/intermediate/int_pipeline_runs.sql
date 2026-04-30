with source as (

    select * from {{ ref('stg_orchestra__pipeline_runs') }}

),

final as (

    select
        pipeline_run_id,
        pipeline_id,
        pipeline_name,
        run_status,
        triggered_by,
        git_branch,
        git_commit_sha,
        created_at_utc,
        started_at_utc,
        updated_at_utc,
        completed_at_utc,
        duration_seconds,
        is_successful,
        is_failed,
        is_in_progress,
        is_cancelled,
        has_warning,
        _dlt_load_id,
        _dlt_id
    from source

)

select * from final
