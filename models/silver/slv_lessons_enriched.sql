{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

with lessons as (
    select * from {{ ref('stg_fact_lesson') }}
),

dim_lesson as (
    select * from {{ ref('stg_dim_lesson') }}
),

relationships as (
    select * from {{ ref('stg_dim_relationship') }}
),

subjects as (
    select * from {{ ref('stg_dim_subject') }}
),

enriched as (
    select
        -- lesson keys
        f.lessonsb_id,
        f.lesson_id,

        -- dates
        f.start_at,
        f.finish_at,
        f.lesson_date,

        -- status
        f.status,
        case when f.status = 'completed' then true else false end   as is_completed,

        -- relationship
        r.relationship_id,
        r.student_id,
        r.tutor_id,

        -- subject
        s.subject_id,
        s.core_subject_type,
        s.roll_up_to

    from lessons f
    left join dim_lesson     dl on f.lesson_id        = dl.dim_lesson_id
    left join relationships  r  on dl.relationship_id = r.relationship_id
    left join subjects       s  on dl.subject_id      = s.subject_id
)

select * from enriched