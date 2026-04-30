with source as (

    select * from {{ source('orchestra', 'operations') }}

),

{% if execute %}
    {% set source_columns = adapter.get_columns_in_relation(source('orchestra', 'operations')) %}
    {% set source_column_names = source_columns | map(attribute='name') | map('lower') | list %}
{% else %}
    {% set source_column_names = [] %}
{% endif %}

{% if 'created_at' in source_column_names %}
    {% set created_at_expr = 'created_at' %}
{% elif 'inserted_at' in source_column_names %}
    {% set created_at_expr = 'inserted_at' %}
{% else %}
    {% set created_at_expr = 'null' %}
{% endif %}

mapped as (

    -- Normalise raw provider-specific operation_status values to the canonical
    -- set. Any unrecognised value is coalesced to UNKNOWN so downstream tests
    -- remain stable regardless of new provider statuses added in the future.
    select
        *,
        coalesce(
            case
                when upper(operation_status) in (
                    'CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'WARNING',
                    'FAILED', 'SKIPPED', 'UNKNOWN', 'CANCELLING', 'CANCELING',
                    'CANCELLED'
                ) then upper(operation_status)
                when upper(operation_status) = 'CANCELED' then 'CANCELLED'
            end,
            'UNKNOWN'
        ) as operation_status_canonical
    from source

),

renamed as (

    select
        -- ids
        id as operation_id,
        task_run_id,
        external_id,

        -- attributes
        operation_name,
        operation_status_canonical as operation_status,
        operation_type,
        integration,
        integration_job,

        -- metrics
        rows_affected,
        operation_duration as duration_seconds,

        -- timestamps
        {{ created_at_expr }} as created_at_utc,

        -- status flags
        operation_status_canonical = 'SUCCEEDED' as is_successful,
        operation_status_canonical = 'FAILED' as is_failed,
        operation_status_canonical = 'SKIPPED' as is_skipped,
        operation_status_canonical = 'WARNING' as has_warning,
        operation_status_canonical in ('CANCELLED', 'CANCELING', 'CANCELLING') as is_cancelled,

        -- operation type flags
        operation_type in ('INGESTION', 'SOURCE') as is_ingestion_operation,
        operation_type in ('MATERIALISATION', 'QUERY', 'AGGREGATION') as is_transformation_operation,
        operation_type in ('TEST', 'TEST_GROUP', 'ANALYSIS') as is_testing_operation,
        operation_type in ('DEPLOY', 'SNAPSHOT', 'SEED') as is_deployment_operation,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from mapped

)

select * from renamed
