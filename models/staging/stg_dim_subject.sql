{{
    config(
        materialized='view',
        tags=['staging', 'dim']
    )
}}

with source as (
    select * from {{ ref('dim_subject') }}  --because we are using dbt seed - we do not seed a sources file for this exercise we can use references alone.
)

select
    id                  as subject_id,
    core_subject_type,
    left,                
    right,
    roll_up_to
from source