{{
    config(
        materialized='view',
        tags=['staging', 'fact']
    )
}}

with source as (
    select * from {{ ref('fact_lesson') }} --because we are using dbt seed - we do not seed a sources file for this exercise we can use references alone.
)

select
    lessonsb_id,
    lesson_id,
    start_at,
    finish_at,
    status,
    date(start_at) as lesson_date
from source