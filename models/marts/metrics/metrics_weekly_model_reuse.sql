{{
    config(
        materialized='incremental',
        unique_key='run_week',
        incremental_strategy='merge'
    )
}}

-- Weekly dbt model build vs. reuse stats across all accounts.
-- Always recomputes a trailing 30-day window (merged into history week-by-week)
-- so late-arriving operations keep refining the most recent weeks.

with model_ops as (

    select
        {{ truncate_to_week('o.created_date') }} as run_week,
        o.operation_name,
        o.operation_status,
        o.duration_seconds

    from {{ ref('fct_operations') }} as o

    where o.integration = 'DBT_CORE'
      and o.operation_type = 'MATERIALISATION'
      and o.created_at_utc >= {{ days_ago(30) }}

),

avg_build as (

    select
        run_week,
        operation_name,
        avg(duration_seconds) as avg_duration_seconds

    from model_ops
    where operation_status = 'SUCCEEDED'
      and duration_seconds > 0
    group by 1, 2

),

final as (

    select
        m.run_week,

        -- run counts
        sum(case when m.operation_status = 'REUSED' then 1 else 0 end) as models_reused,
        sum(case when m.operation_status = 'SUCCEEDED' then 1 else 0 end) as models_built,

        -- reuse rate
        round(
            100.0 * sum(case when m.operation_status = 'REUSED' then 1 else 0 end)
            / nullif(sum(case when m.operation_status in ('REUSED', 'SUCCEEDED') then 1 else 0 end), 0),
        1) as reuse_rate_pct,

        -- distinct models reused
        count(distinct case when m.operation_status = 'REUSED' then m.operation_name end)
            as distinct_models_reused,

        -- compute time saved: each reused model would otherwise have taken
        -- roughly as long as that model's average successful build that week
        round(
            sum(case when m.operation_status = 'REUSED' then a.avg_duration_seconds end) / 60.0,
        1) as estimated_compute_minutes_saved,

        current_timestamp as updated_at

    from model_ops as m
    left join avg_build as a
        on m.run_week = a.run_week
        and m.operation_name = a.operation_name
    group by 1

)

select * from final
