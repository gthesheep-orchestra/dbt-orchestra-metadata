with source as (

    select * from {{ source('orchestra', 'assets') }}

),

renamed as (

    select
        -- ids
        asset_id,
        external_id,

        -- attributes
        asset_name,
        asset_type,
        integration,
        status as asset_status,

        -- dependencies (kept as JSON for downstream parsing)
        {{ source_column_or_null(source('orchestra', 'assets'), 'upstream_dependencies') }} as upstream_dependencies,
        {{ source_column_or_null(source('orchestra', 'assets'), 'downstream_dependencies') }} as downstream_dependencies,

        -- metrics
        row_count,
        bytes as size_bytes,
        case
            when bytes is not null then bytes / 1073741824.0  -- 1024^3
        end as size_gb,

        -- timestamps
        created_in_integration as created_at_utc,

        -- asset type flags
        asset_type in ('TABLE', 'VIEW') as is_database_object,
        asset_type in ('DASHBOARD', 'DASHBOARD_VIEWS', 'WORKBOOK') as is_bi_asset,
        asset_type in ('DATASET', 'QUERIES') as is_query_asset,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
