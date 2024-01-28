with 

source as (

    select * from {{ source('clara', 'repos') }}

),

unique_entity as (

  select distinct -- deleting dups

    id as repo_id,
    name as repo_name

  from source

),

rownumber as (

    select

        repo_id,
        repo_name,
        row_number() over(partition by repo_id order by repo_name) as rn -- a better criteria is needed to define rn
    
    from unique_entity

)

select * from rownumber