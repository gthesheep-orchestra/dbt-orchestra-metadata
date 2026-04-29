with src as (

    select
        upper(trim(operation_status)) as op_status_raw,
        t.* except(operation_status)
    from {{ source('orchestra', 'operations') }} as t

),

normalized as (

    select
        case
            when op_status_raw in ('CANCELING', 'CANCELLING', 'CANCELED') then 'CANCELLED'
            when op_status_raw in ('SUCCESS', 'COMPLETED', 'COMPLETE') then 'SUCCEEDED'
            when op_status_raw in ('ERROR') then 'FAILED'
            else op_status_raw
        end as operation_status,
        * except(op_status_raw)
    from src

),

renamed as (

    select
        -- ids
        id as operation_id,
        task_run_id,
        external_id,

        -- attributes
        operation_name,
        operation_status,
        operation_type,
        integration,
        integration_job,

        -- metrics
        rows_affected,
        operation_duration as duration_seconds,

        -- timestamps
        created_at as created_at_utc,

        -- status flags
        operation_status = 'SUCCEEDED' as is_successful,
        operation_status = 'FAILED' as is_failed,
        operation_status = 'SKIPPED' as is_skipped,
        operation_status = 'WARNING' as has_warning,
        operation_status = 'CANCELLED' as is_cancelled,

        -- operation type flags
        operation_type in ('INGESTION', 'SOURCE') as is_ingestion_operation,
        operation_type in ('MATERIALISATION', 'QUERY', 'AGGREGATION') as is_transformation_operation,
        operation_type in ('TEST', 'TEST_GROUP', 'ANALYSIS') as is_testing_operation,
        operation_type in ('DEPLOY', 'SNAPSHOT', 'SEED') as is_deployment_operation,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from normalized

)

select * from renamed
