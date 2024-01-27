with 

source as (

    select * from {{ source('clara', 'actors') }}

),

unique_entity as (

  select distinct -- deleting dups

    id as actor_id,
    username as actor_username,

  from source

),

rownumber as (

    select

        actor_id,
        actor_username,
        row_number() over(partition by actor_id order by actor_username) as rn -- a better criteria is needed to define rn
    
    from unique_entity

)

select * from rownumber