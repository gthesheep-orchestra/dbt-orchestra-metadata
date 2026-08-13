{{
    config(
        materialized='incremental',
        unique_key='run_week',
        incremental_strategy='merge'
    )
}}

-- Weekly state-aware orchestration (SAO) adoption for dbt Core tasks across
-- all accounts. Always recomputes a trailing 180-day window.

with task_runs as (

    select
        {{ truncate_to_week('tr.created_date') }} as run_week,
        tr.unique_task_id,
        tr.is_state_orchestration_enabled

    from {{ ref('fct_task_runs') }} as tr
    inner join {{ ref('stg_orchestra__pipelines') }} as p
        on tr.pipeline_id = p.pipeline_id
        and not p.is_deleted

    where tr.integration = 'DBT_CORE'
      and tr.created_at_utc >= {{ days_ago(180) }}
      and not tr.is_matrix_parent

),

final as (

    select
        run_week,

        count(distinct case when is_state_orchestration_enabled then unique_task_id end)
            as sao_enabled_tasks,
        count(distinct unique_task_id) as total_dbt_core_tasks,

        round(
            100.0 * count(distinct case when is_state_orchestration_enabled then unique_task_id end)
            / nullif(count(distinct unique_task_id), 0),
        1) as sao_adoption_pct,

        sum(case when is_state_orchestration_enabled then 1 else 0 end) as sao_task_runs,

        current_timestamp as updated_at

    from task_runs
    group by 1

)

select * from final
