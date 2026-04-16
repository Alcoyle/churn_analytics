{{
    config(
        materialized='view',
        tags=['staging', 'dim']
    )
}}

with source as (
    select * from {{ ref('dim_relationship') }} --because we are using dbt seed - we do not seed a sources file for this exercise we can use references alone.
)

select
    id          as relationship_id,
    tutor_id,
    student_id
from source