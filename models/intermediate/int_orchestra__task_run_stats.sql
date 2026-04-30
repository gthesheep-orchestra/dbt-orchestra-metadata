{{
    config(
        materialized='view'
    )
}}

with task_runs as (

    select * from {{ ref('stg_orchestra__task_runs') }}

),

final as (

    select
        task_run_id,
        pipeline_run_id,
        task_name,
        is_successful,
        is_failed,
        is_skipped,
        has_warning,
        duration_seconds,
        integration,
        integration_job

    from task_runs

)

select * from final
