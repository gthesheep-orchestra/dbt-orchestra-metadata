with source as (

    select * from {{ ref('stg_orchestra__operations') }}

),

final as (

    select
        operation_id,
        task_run_id,
        external_id,
        operation_name,
        operation_status,
        operation_type,
        integration,
        integration_job,
        rows_affected,
        duration_seconds,
        created_at_utc,
        is_successful,
        is_failed,
        is_skipped,
        has_warning,
        is_cancelled,
        is_ingestion_operation,
        is_transformation_operation,
        is_testing_operation,
        is_deployment_operation,
        _dlt_load_id,
        _dlt_id
    from source

)

select * from final
