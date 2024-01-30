with

actors as (

   select * 
   from {{ ref('stg_clara__actors') }}
   where rn = 1

),

commits as (

   select * from {{ ref('stg_clara__commits') }}

),

events as (

   select * from {{ ref('stg_clara__events') }}

),

repos as (

   select * 
   from {{ ref('stg_clara__repos') }}
   where rn = 1

),

commits_join_actors_events_and_repos as (

   select
      c.event_id,
      c.sha,
      c.message,

      e.event_type,
      e.actor_id,
      a.actor_username,

      e.repo_id,
      r.repo_name,

   from commits as c

   left join events as e on c.event_id = e.event_id -- a full-join, left-join, right-join can be used. We need tech to define the logic. I've decided to use a left join under the premise that all commits should have metadata (SHA, commit message, etc.) Also, the loader needs to be checked and find out why so many commits don't have metadata.
   left join actors as a on e.actor_id = a.actor_id
   left join repos as r on e.repo_id = r.repo_id

)

select * from commits_join_actors_events_and_repos