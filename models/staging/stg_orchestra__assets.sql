with source as (

    select * from {{ source('orchestra', 'assets') }}

),

-- Normalise vendor-specific asset_type values into the canonical set before
-- any downstream tests or models consume the column.
normalized as (

    select
        asset_id,
        external_id,
        asset_name,
        case
            when upper(asset_type) in (
                'DASHBOARD', 'DASHBOARD_VIEWS', 'DATASET', 'QUERIES',
                'TABLE', 'UNKNOWN', 'VIEW', 'WORKBOOK'
            ) then upper(asset_type)
            -- Common vendor-specific variants
            when upper(asset_type) = 'DASHBOARD_VIEW'  then 'DASHBOARD_VIEWS'
            when upper(asset_type) = 'QUERY'           then 'QUERIES'
            when upper(asset_type) in ('BASE TABLE', 'MANAGED TABLE',
                                       'EXTERNAL TABLE')  then 'TABLE'
            when upper(asset_type) in ('MATERIALIZED VIEW',
                                       'SECURE VIEW')     then 'VIEW'
            when upper(asset_type) in ('REPORT', 'TILE')  then 'DASHBOARD'
            else 'UNKNOWN'
        end                                            as asset_type,
        integration,
        status,
        upstream_dependencies,
        downstream_dependencies,
        row_count,
        bytes,
        created_in_integration,
        _dlt_load_id,
        _dlt_id
    from source

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
        {{
            source_column_or_null(source('orchestra', 'assets'), 'downstream_dependencies')
        }} as downstream_dependencies,

        -- metrics
        row_count,
        bytes as size_bytes,
        case
            when bytes is not null then bytes / 1073741824.0  -- 1024^3
        end as size_gb,

        -- timestamps
        created_in_integration as created_at_utc,

        -- asset type flags (evaluated against the normalised asset_type)
        asset_type in ('TABLE', 'VIEW') as is_database_object,
        asset_type in ('DASHBOARD', 'DASHBOARD_VIEWS', 'WORKBOOK') as is_bi_asset,
        asset_type in ('DATASET', 'QUERIES') as is_query_asset,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from normalized

)

select * from renamed
