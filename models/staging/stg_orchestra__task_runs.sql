with source as (

    select * from {{ source('orchestra', 'task_runs') }}

),

renamed as (

    select
        -- ids
        id as task_run_id,
        pipeline_run_id,
        {{ source_column_or_null(source('orchestra', 'task_runs'), 'unique_task_id') }} as unique_task_id,

        -- attributes
        task_name,
        status as task_status,
        integration,
        integration_job,
        message as status_message,
        external_status,
        account_id,
        matrix_parent as is_matrix_parent,

        -- feature flags parsed from task_parameters. dlt flattens the task_parameters
        -- JSON object into task_parameters__<key> columns rather than keeping it as a
        -- single JSON/string column, so read the flattened boolean directly.
        {{ source_column_or_null(source('orchestra', 'task_runs'), 'task_parameters__use_state_orchestration') }} as is_state_orchestration_enabled,

        -- timestamps. dlt sometimes infers these as STRING instead of TIMESTAMP
        -- (e.g. from early null/malformed loads), so cast defensively rather
        -- than assume the source column is already typed as TIMESTAMP.
        {{ safe_cast_timestamp('created_at') }} as created_at_utc,
        {{ safe_cast_timestamp('started_at') }} as started_at_utc,
        {{ safe_cast_timestamp('updated_at') }} as updated_at_utc,
        {{ safe_cast_timestamp('completed_at') }} as completed_at_utc,

        -- calculated fields
        case
            when completed_at is not null and started_at is not null
                then {{ timestamp_diff_seconds('completed_at', 'started_at') }}
        end as duration_seconds,

        -- status flags
        status = 'SUCCEEDED' as is_successful,
        status = 'FAILED' as is_failed,
        status in ('RUNNING', 'QUEUED', 'CREATED') as is_in_progress,
        status in ('CANCELLED', 'CANCELLING') as is_cancelled,
        status = 'SKIPPED' as is_skipped,
        status = 'WARNING' as has_warning,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
