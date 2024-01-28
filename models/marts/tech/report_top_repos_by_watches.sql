with
events as (

    select * from {{ ref('stg_clara__events') }}

),

repos as (

    select * from {{ ref('stg_clara__repos') }}

),

watch_count_by_repo as (

    select
        repo_id,
        count(1) as n_watch_events,

    from events

    where event_type = 'WatchEvent'

    group by repo_id

),

final as (

    select
        wc.repo_id,
        r.repo_name,
        wc.n_watch_events,
        rank() over(order by wc.n_watch_events desc) as ranking

    from watch_count_by_repo as wc

    left join repos as r on wc.repo_id = r.repo_id

    order by ranking

    limit 10

)

select * from final
