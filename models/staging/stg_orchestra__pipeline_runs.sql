with source as (

    select * from {{ source('orchestra', 'pipeline_runs') }}

),

renamed as (

    select
        -- ids
        id as pipeline_run_id,
        pipeline_id,

        -- attributes
        pipeline_name,
        run_status,
        {{ source_column_or_null(source('orchestra', 'pipeline_runs'), 'triggered_by') }} as triggered_by,
        branch as git_branch,
        commit as git_commit_sha,
        account_id,

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
        run_status = 'SUCCEEDED' as is_successful,
        run_status = 'FAILED' as is_failed,
        run_status in ('RUNNING', 'CREATED') as is_in_progress,
        run_status in ('CANCELLED', 'CANCELLING') as is_cancelled,
        run_status = 'WARNING' as has_warning,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
