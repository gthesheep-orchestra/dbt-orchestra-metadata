{{
    config(
        materialized='table',
        unique_key='asset_id'
    )
}}

with assets as (

    select * from {{ ref('int_assets') }}

),

final as (

    select
        asset_id,
        external_id,
        asset_name,
        asset_type,
        integration,
        asset_status,

        -- dependency counts (parse JSON arrays)
        coalesce({{ json_array_length('upstream_dependencies') }}, 0) as upstream_dependency_count,
        coalesce({{ json_array_length('downstream_dependencies') }}, 0) as downstream_dependency_count,

        -- raw dependencies for lineage analysis
        upstream_dependencies,
        downstream_dependencies,

        -- metrics
        row_count,
        size_bytes,
        size_gb,

        -- categorization
        is_database_object,
        is_bi_asset,
        is_query_asset,

        -- determine if this is a source (no upstream) or terminal (no downstream)
        coalesce({{ json_array_length('upstream_dependencies') }}, 0) = 0 as is_source_asset,
        coalesce({{ json_array_length('downstream_dependencies') }}, 0) = 0 as is_terminal_asset,

        -- timestamps
        created_at_utc,
        current_timestamp as updated_at

    from assets

)

select * from final
