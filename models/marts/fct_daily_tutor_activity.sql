{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

with lessons as (
    select * from {{ ref('slv_lessons_enriched') }}
),

events as (
    select * from {{ ref('slv_churn_reactivation_events') }}
),

daily_lessons as (
    select
        lesson_date         as activity_date,
        tutor_id,
        count(*)                                        as total_lessons,
        countif(is_completed)                           as completed_lessons,
        count(distinct student_id)                      as unique_students
    from lessons
    group by 1, 2
),

daily_events as (
    select
        event_date          as activity_date,
        tutor_id,
        countif(event_type = 'churn')           as student_churns,
        countif(event_type = 'reactivation')    as student_reactivations
    from events
    group by 1, 2
)

select
    coalesce(l.activity_date, e.activity_date)  as activity_date,
    coalesce(l.tutor_id, e.tutor_id)            as tutor_id,
    coalesce(l.total_lessons, 0)                as total_lessons,
    coalesce(l.completed_lessons, 0)            as completed_lessons,
    coalesce(l.unique_students, 0)              as unique_students,
    coalesce(e.student_churns, 0)               as student_churns,
    coalesce(e.student_reactivations, 0)        as student_reactivations
from daily_lessons      l
full outer join daily_events e
    on  l.activity_date = e.activity_date
    and l.tutor_id      = e.tutor_id
order by 1, 2