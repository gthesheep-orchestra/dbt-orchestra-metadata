with source as (

    select * from {{ source('orchestra', 'pipelines') }}

),

renamed as (

    select
        -- ids
        pipeline_id,
        account_id,

        -- attributes
        pipeline_name,
        is_deleted,

        -- timestamps
        created_at as created_at_utc,
        updated_at as updated_at_utc,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from renamed
