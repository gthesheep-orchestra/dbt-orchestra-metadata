{{
    config(
        materialized='table'
    )
}}

-- A table, not incremental: incidents are a full snapshot and change status after opening.

with incidents as (

    select * from {{ ref('stg_orchestra__incidents') }}

),

-- An incident lists no pipeline itself; its events point at the runs that raised it.
first_pipeline_events as (

    select
        incident_id,
        pipeline_id,
        pipeline_run_id
    from (
        select
            incident_id,
            pipeline_id,
            pipeline_run_id,
            row_number() over (partition by incident_id order by inserted_at_utc) as event_number
        from {{ ref('stg_orchestra__incident_events') }}
        where pipeline_id is not null
    ) as pipeline_events
    where event_number = 1

),

pipeline_context as (

    select
        pipeline_run_id,
        pipeline_name
    from {{ ref('stg_orchestra__pipeline_runs') }}

),

final as (

    select
        -- ids
        i.incident_id,
        fpe.pipeline_id,
        fpe.pipeline_run_id,
        i.monitor_id,

        -- attributes
        i.incident_name,
        i.incident_status,
        i.severity,
        i.assignee,

        -- context from the pipeline run that raised the incident
        pc.pipeline_name,

        -- timestamps
        i.created_at_utc,
        i.updated_at_utc,
        i.resolved_at_utc,
        i.archived_at_utc,

        -- date keys for dimensional analysis
        cast(i.created_at_utc as date) as created_date,
        cast(i.resolved_at_utc as date) as resolved_date,

        -- dismissed incidents also get a resolved time, but weren't fixed
        case
            when i.is_resolved
                then {{ timestamp_diff_seconds('i.resolved_at_utc', 'i.created_at_utc') }}
        end as time_to_resolve_seconds,

        -- status flags
        i.is_open,
        i.is_resolved,
        i.is_dismissed,
        i.archived_at_utc is not null as is_archived,

        -- dlt metadata
        i._dlt_load_id,
        i._dlt_id

    from incidents as i
    left join first_pipeline_events as fpe on i.incident_id = fpe.incident_id
    left join pipeline_context as pc on fpe.pipeline_run_id = pc.pipeline_run_id

)

select * from final
