with 

source as (

    select * from {{ source('clara', 'repos') }}

),

transformed as (

  select 

    id as repo_id,
    name as repo_name
    
  from source

)

select * from transformed