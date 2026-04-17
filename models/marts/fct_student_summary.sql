{{
    config(
        materialized='table',
        tags=['marts', 'dim']
    )
}}

with lessons as (
    select * from {{ ref('slv_lessons_enriched') }}
),

events as (
    select * from {{ ref('slv_churn_reactivation_events') }}
),

lesson_stats as (
    select
        student_id,
        count(*)                            as total_lessons,
        countif(is_completed)               as completed_lessons,
        min(lesson_date)                    as first_lesson_date,
        max(lesson_date)                    as last_lesson_date,
        date_diff(
            current_date(),
            max(lesson_date),
            day
        )                                   as days_since_last_lesson,
        count(distinct tutor_id)            as total_tutors,
        count(distinct core_subject_type)   as total_subjects
    from lessons
    group by 1
),

event_stats as (
    select
        student_id,
        countif(event_type = 'churn')           as total_churn_events,
        countif(event_type = 'reactivation')    as total_reactivation_events,
        max(case when event_type = 'churn'
            then event_date end)                as last_churn_date,
        max(case when event_type = 'reactivation'
            then event_date end)                as last_reactivation_date
    from events
    group by 1
)

select
    l.student_id,
    l.total_lessons,
    l.completed_lessons,
    l.first_lesson_date,
    l.last_lesson_date,
    l.days_since_last_lesson,
    l.total_tutors,
    l.total_subjects,
    coalesce(e.total_churn_events, 0)           as total_churn_events,
    coalesce(e.total_reactivation_events, 0)    as total_reactivation_events,
    e.last_churn_date,
    e.last_reactivation_date,
    case
        when l.days_since_last_lesson > 30  then 'churned'
        when l.days_since_last_lesson <= 30 then 'active'
    end                                         as current_status
from lesson_stats       l
left join event_stats   e on l.student_id = e.student_id
