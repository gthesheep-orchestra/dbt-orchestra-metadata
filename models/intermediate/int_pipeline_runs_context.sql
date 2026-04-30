with pipeline_runs as (

    select * from {{ ref('stg_orchestra__pipeline_runs') }}

)

select
    pipeline_run_id,
    pipeline_id,
    pipeline_name,
    git_branch,
    git_commit_sha

from pipeline_runs
