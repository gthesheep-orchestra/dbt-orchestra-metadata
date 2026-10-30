with source as (

    select * from {{ source('orchestra', 'incidents') }}

),

renamed as (

    select
        -- ids
        incident_id,
        {{ source_column_or_null(source('orchestra', 'incidents'), 'monitor_id') }} as monitor_id,

        -- attributes
        name as incident_name,
        status as incident_status,
        severity,
        {{ source_column_or_null(source('orchestra', 'incidents'), 'assignee') }} as assignee,

        -- timestamps, cast defensively since dlt sometimes infers them as STRING
        {{ safe_cast_timestamp('created_at') }} as created_at_utc,
        {{ safe_cast_timestamp('updated_at') }} as updated_at_utc,
        {{ safe_cast_timestamp(source_column_or_null(source('orchestra', 'incidents'), 'resolved_at')) }}
            as resolved_at_utc,
        {{ safe_cast_timestamp(source_column_or_null(source('orchestra', 'incidents'), 'archived_at')) }}
            as archived_at_utc,

        -- status flags
        status in ('OPEN', 'INVESTIGATING') as is_open,
        status = 'RESOLVED' as is_resolved,
        status = 'DISMISSED' as is_dismissed,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
