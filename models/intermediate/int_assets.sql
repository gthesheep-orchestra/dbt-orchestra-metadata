with source as (

    select * from {{ ref('stg_orchestra__assets') }}

),

final as (

    select
        asset_id,
        external_id,
        asset_name,
        asset_type,
        integration,
        asset_status,
        upstream_dependencies,
        downstream_dependencies,
        row_count,
        size_bytes,
        size_gb,
        created_at_utc,
        is_database_object,
        is_bi_asset,
        is_query_asset,
        _dlt_load_id,
        _dlt_id
    from source

)

select * from final
