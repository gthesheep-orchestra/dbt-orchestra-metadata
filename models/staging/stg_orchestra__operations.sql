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

normalized as (

    select
        *,
        upper(coalesce(nullif(trim(operation_status), ''), 'UNKNOWN')) as operation_status_normalized,
        upper(coalesce(nullif(trim(operation_type), ''), 'UNKNOWN')) as operation_type_normalized
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
        operation_status_normalized as operation_status,
        operation_type_normalized as operation_type,
        integration,
        integration_job,

        -- metrics
        rows_affected,
        operation_duration as duration_seconds,

        -- timestamps
        {{ created_at_expr }} as created_at_utc,

        -- status flags
        operation_status_normalized = 'SUCCEEDED' as is_successful,
        operation_status_normalized = 'FAILED' as is_failed,
        operation_status_normalized = 'SKIPPED' as is_skipped,
        operation_status_normalized = 'WARNING' as has_warning,
        operation_status_normalized in ('CANCELLED', 'CANCELING', 'CANCELLING') as is_cancelled,

        -- operation type flags
        operation_type_normalized in ('INGESTION', 'SOURCE') as is_ingestion_operation,
        operation_type_normalized in ('MATERIALISATION', 'QUERY', 'AGGREGATION') as is_transformation_operation,
        operation_type_normalized in ('TEST', 'TEST_GROUP', 'ANALYSIS') as is_testing_operation,
        operation_type_normalized in ('DEPLOY', 'SNAPSHOT', 'SEED') as is_deployment_operation,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from normalized

)

select * from renamed
