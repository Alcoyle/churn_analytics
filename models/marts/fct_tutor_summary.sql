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
        tutor_id,
        count(*)                            as total_lessons,
        countif(is_completed)               as completed_lessons,
        count(distinct student_id)          as total_unique_students,
        min(lesson_date)                    as first_lesson_date,
        max(lesson_date)                    as last_lesson_date
    from lessons
    group by 1
),

event_stats as (
    select
        tutor_id,
        count(distinct case when event_type = 'churn'
            then student_id end)            as total_churned_students,
        count(distinct case when event_type = 'reactivation'
            then student_id end)            as total_reactivated_students,
        countif(event_type = 'churn')       as total_churn_events,
        countif(event_type = 'reactivation') as total_reactivation_events
    from events
    group by 1
)

select
    l.tutor_id,
    l.total_lessons,
    l.completed_lessons,
    l.total_unique_students,
    l.first_lesson_date,
    l.last_lesson_date,
    coalesce(e.total_churned_students, 0)       as total_churned_students,
    coalesce(e.total_reactivated_students, 0)   as total_reactivated_students,
    coalesce(e.total_churn_events, 0)           as total_churn_events,
    coalesce(e.total_reactivation_events, 0)    as total_reactivation_events,
    round(
        safe_divide(
            coalesce(e.total_churned_students, 0),
            l.total_unique_students
        ) * 100, 2
    )                                           as churn_rate_pct
from lesson_stats       l
left join event_stats   e on l.tutor_id = e.tutor_id
order by l.total_unique_students desc