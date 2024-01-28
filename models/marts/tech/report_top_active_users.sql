with

commits as (

    select * from {{ ref('int_commits_joined') }}

),

events as (

    select * from {{ ref('stg_clara__events') }}

),

commit_count_by_actor as (

    select
        actor_id,
        actor_username,
        count(1) as n_commits,

    from commits

    group by
        actor_id,
        actor_username

),

pr_count_by_actor as (

    select
        actor_id,
        sum(if(event_type = 'PullRequestEvent', 1, 0)) as n_prs,

    from events

    where event_type = 'PullRequestEvent'

    group by actor_id

),

joining_commit_count_and_pr_count as (
    
    select
        cc.actor_id,
        cc.actor_username,
        cc.n_commits,
        pc.n_prs,
        cc.n_commits + pc.n_prs as n_commits_and_prs,

    from commit_count_by_actor as cc

    left join pr_count_by_actor as pc on cc.actor_id = pc.actor_id

),

ranking_cte as (

    select
        actor_id,
        actor_username,
        n_commits,
        n_prs,
        n_commits_and_prs,
        rank() over(order by n_commits_and_prs desc) as ranking

    from joining_commit_count_and_pr_count

    order by ranking

    limit 10

)

select * from ranking_cte


-- Note: there are many commits that appear in the `stg_clara__events` model that don't appear in the `stg_clara__commits` model (2139 to be precise).
    -- One example is the actor "LombiqBot" (actor_id = 8517910). That actor has 1529 PushEvents that are not registered in the `stg_clara__commits` model.
    -- Therefore, based on the `int_commits_joined` model it will not appear as a top 10 user.
    -- In contrast, if the `stg_clara__events` were to be used, it would appear as the number 1 most active user.
    -- The model `int_commits_joined` will be prefered over `stg_clara__events` as the Source-of-Truth when dealing with commit info, since it contains more information such as SHA and commit message. 