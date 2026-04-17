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
    Indentification as subject_id,
    `Core Subject Type` as core_subject_type, --backticks to help parse the spaces in the column titles
    `left` as left_num,                
    `right` as right_num,
   `Roll Up To` AS roll_up_to
from source