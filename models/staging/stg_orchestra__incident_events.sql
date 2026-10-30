with source as (

    select * from {{ source('orchestra', 'incident_events') }}

),

renamed as (

    select
        -- ids
        incident_event_id,
        incident_id,
        {{ source_column_or_null(source('orchestra', 'incident_events'), 'pipeline_id') }} as pipeline_id,
        {{ source_column_or_null(source('orchestra', 'incident_events'), 'pipeline_run_id') }} as pipeline_run_id,
        {{ source_column_or_null(source('orchestra', 'incident_events'), 'task_run_id') }} as task_run_id,

        -- attributes
        event_type,
        actor_type,
        {{ source_column_or_null(source('orchestra', 'incident_events'), 'trigger_type') }} as trigger_type,

        -- timestamps, cast defensively since dlt sometimes infers them as STRING
        {{ safe_cast_timestamp('inserted_at') }} as inserted_at_utc,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
