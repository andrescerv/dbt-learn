with

actors as (
    
    select
        actor_id,
        actor_username 
    from {{ ref('stg_clara__actors') }}

),

events as (

    select
        event_id,
        event_type,
        actor_id,
        repo_id
    from {{ ref('stg_clara__events') }}

),

prs as (

    select
        actor_id,
        count(1) as prs_and_commits,
        sum(if(event_type = 'PullRequestEvent', 1, 0)) as prs,
        sum(if(event_type = 'PushEvent', 1, 0)) as commits,
    from events
    where event_type in (
        'PullRequestEvent',
        'PushEvent'
    )

    group by actor_id

),

final as (
    
    select
        a.actor_id,
        a.actor_username,
        pc.prs,
        pc.commits,
        pc.prs_and_commits,
    from actors as a
    left join prs as pc on a.actor_id = pc.actor_id
)

select *
from final
order by prs_and_commits desc
limit 10