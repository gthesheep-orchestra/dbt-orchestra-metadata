{{
    config(
        materialized='incremental',
        unique_key='operation_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with operations as (

    select * from {{ ref('stg_orchestra__operations') }}

    {% if is_incremental() %}
    where created_at_utc > (select max(created_at_utc) from {{ this }})
    {% endif %}

),

task_context as (

    select
        task_run_id,
        pipeline_run_id,
        task_name
    from {{ ref('stg_orchestra__task_runs') }}

),

pipeline_context as (

    select
        pipeline_run_id,
        pipeline_id,
        pipeline_name,
        git_branch,
        git_commit_sha
    from {{ ref('int_orchestra__pipeline_context') }}

),

final as (

    select
        -- ids
        o.operation_id,
        o.task_run_id,
        tc.pipeline_run_id,
        pc.pipeline_id,
        o.external_id,

        -- integration key for joining to dim_integrations
        {{ dbt_utils.generate_surrogate_key(['o.integration', 'o.integration_job']) }} as integration_key,

        -- attributes
        o.operation_name,
        o.operation_status,
        o.operation_type,
        o.integration,
        o.integration_job,

        -- context from parent entities
        tc.task_name,
        pc.pipeline_name,
        pc.git_branch,
        pc.git_commit_sha,

        -- timestamps
        o.created_at_utc,

        -- date key for dimensional analysis
        cast(o.created_at_utc as date) as created_date,

        -- metrics
        o.rows_affected,
        o.duration_seconds,
        o.duration_seconds / 60.0 as duration_minutes,

        -- rows per second throughput
        case
            when o.duration_seconds > 0 and o.rows_affected is not null
                then o.rows_affected * 1.0 / o.duration_seconds
        end as rows_per_second,

        -- status flags
        o.is_successful,
        o.is_failed,
        o.is_skipped,
        o.has_warning,
        o.is_cancelled,

        -- operation type flags
        o.is_ingestion_operation,
        o.is_transformation_operation,
        o.is_testing_operation,
        o.is_deployment_operation,

        -- dlt metadata
        o._dlt_load_id,
        o._dlt_id

    from operations as o
    left join task_context as tc on o.task_run_id = tc.task_run_id
    left join pipeline_context as pc on tc.pipeline_run_id = pc.pipeline_run_id

)

select * from final
