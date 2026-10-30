{{
    config(
        materialized='incremental',
        unique_key='run_week',
        incremental_strategy='merge'
    )
}}

-- Weekly pipeline run duration (avg/p50/p90) across all accounts.
-- Builds a trailing 180-day window, then recomputes the previous and current week.

with pipeline_runs as (

    select
        {{ truncate_to_week('pr.completed_date') }} as run_week,
        pr.duration_seconds

    from {{ ref('fct_pipeline_runs') }} as pr

    where pr.started_at_utc is not null
    {% if is_incremental() %}
      and pr.completed_date >= (select max(run_week) - interval 7 day from {{ this }})
    {% else %}
      and pr.completed_at_utc >= {{ days_ago(180) }}
    {% endif %}

),

final as (

    select
        run_week,

        count(*) as completed_runs,
        round(avg(duration_seconds) / 60.0, 2) as avg_minutes,
        round({{ percentile_cont('duration_seconds', 0.5) }} / 60.0, 2) as p50_minutes,
        round({{ percentile_cont('duration_seconds', 0.9) }} / 60.0, 2) as p90_minutes,

        current_timestamp as updated_at

    from pipeline_runs
    group by 1

)

select * from final
