with 

source as (

    select * from {{ source('clara', 'commits') }}

),

transformed as (

  select 

    event_id,
    sha,
    message

  from source

)

select * from transformed