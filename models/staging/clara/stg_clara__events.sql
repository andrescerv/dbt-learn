with 

source as (

    select * from {{ source('clara', 'events') }}

),

transformed as (

  select 

    id as event_id,
    type as event_type,
    actor_id,
    repo_id

  from source

)

select * from transformed