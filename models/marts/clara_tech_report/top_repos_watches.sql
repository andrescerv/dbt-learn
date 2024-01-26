with

repos as (
    
    select
        repo_id,
        repo_name

    from {{ ref('stg_repos') }}

),

events as (

    select
        event_id,
        event_type,
        actor_id,
        repo_id

    from {{ ref('stg_events') }}

),

prs as (

    select
        repo_id,
        count(1) as watches

    from events
    where event_type in ('WatchEvent')

    group by repo_id

),

final as (
    
    select
        r.repo_id,
        r.repo_name,
        pc.watches,

    from repos as r
    left join prs as pc on r.repo_id = pc.repo_id
)

select *
from final
order by watches desc
limit 10