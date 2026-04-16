{{
    config(
        materialized='view',
        tags=['staging', 'dim']
    )
}}

with source as (
    select * from {{ ref('dim_lesson') }} --because we are using dbt seed - we do not seed a sources file for this exercise we can use references alone.
)


select
    id              as dim_lesson_id,
    relationship_id,
    subject_id
from source