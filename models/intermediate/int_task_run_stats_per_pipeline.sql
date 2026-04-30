with task_runs as (

    select * from {{ ref('stg_orchestra__task_runs') }}

),

final as (

    select
        pipeline_run_id,
        count(*) as total_tasks,
        sum(case when is_successful then 1 else 0 end) as successful_tasks,
        sum(case when is_failed then 1 else 0 end) as failed_tasks,
        sum(case when is_skipped then 1 else 0 end) as skipped_tasks,
        sum(case when has_warning then 1 else 0 end) as warning_tasks,
        sum(duration_seconds) as total_task_duration_seconds,
        count(distinct integration) as unique_integrations

    from task_runs
    group by 1

)

select * from final
