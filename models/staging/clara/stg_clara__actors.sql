with 

source as (

    select * from {{ source('clara', 'actors') }}

),

transformed as (

  select distinct

    id as actor_id,
    username as actor_username

  from source

)

select * from transformed