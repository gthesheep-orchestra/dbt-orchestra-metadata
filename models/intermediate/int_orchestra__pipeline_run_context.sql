{{
    config(
        materialized='view'
    )
}}

with pipeline_runs as (

    select * from {{ ref('stg_orchestra__pipeline_runs') }}

),

final as (

    select
        pipeline_run_id,
        pipeline_id,
        pipeline_name,
        git_branch,
        git_commit_sha

    from pipeline_runs

)

select * from final
