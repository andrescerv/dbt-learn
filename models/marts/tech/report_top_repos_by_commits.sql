with
commits as (

    select * from {{ ref('int_commits_joined') }}

),

commit_count_by_repo as (

    select
        repo_id,
        repo_name,
        count(1) as n_commits,

    from commits

    group by
        repo_id,
        repo_name

),

final as (

    select
        repo_id,
        repo_name,
        n_commits,
        rank() over(order by n_commits desc) as ranking

    from commit_count_by_repo

    order by ranking

    limit 10

)

select * from final
