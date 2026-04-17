{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

with gaps as (
    select * from {{ ref('slv_student_lesson_gaps') }}
),

reactivations as (
    select
        student_id,
        lesson_date                                 as reactivation_date,
        previous_lesson_date                        as churned_from_date,
        days_since_last_lesson,
        tutor_id,
        subject_id,
        core_subject_type
    from gaps
    where days_since_last_lesson > 30
),

last_lessons as (
    -- Identify the actual last lesson for every student/tutor/subject combo
    select
        student_id,
        tutor_id,
        subject_id,
        core_subject_type,
        max(lesson_date) as max_lesson_date
    from {{ ref('slv_lessons_enriched') }}
    where is_completed = true
    group by 1, 2, 3, 4
),

churn_events as (
    -- Gaps-based churn
    select
        student_id,
        date_add(previous_lesson_date, interval 30 day)  as churn_date, 
        previous_lesson_date                             as last_lesson_before_churn,
        lesson_date                                      as return_date,
        days_since_last_lesson,
        tutor_id,
        subject_id,
        core_subject_type
    from gaps
    where days_since_last_lesson > 30

    union all

    -- Final churn (students who haven't returned yet)
    select
        l.student_id,
        date_add(l.max_lesson_date, interval 30 day)    as churn_date,
        l.max_lesson_date                               as last_lesson_before_churn,
        null                                            as return_date,
        null                                            as days_since_last_lesson,
        l.tutor_id,
        l.subject_id,
        l.core_subject_type
    from last_lessons l
    -- Replaced NOT IN with a LEFT JOIN due to bigquery sandbox limitations
    left join reactivations r 
        on l.student_id = r.student_id 
        and r.reactivation_date > date_add(l.max_lesson_date, interval 30 day)
    where date_add(l.max_lesson_date, interval 30 day) <= current_date()
      and r.student_id is null 
)

select
    'churn'         as event_type,
    student_id,
    churn_date      as event_date,
    tutor_id,
    subject_id,
    core_subject_type,
    last_lesson_before_churn,
    return_date,
    days_since_last_lesson
from churn_events

union all

select
    'reactivation'  as event_type,
    student_id,
    reactivation_date as event_date,
    tutor_id,
    subject_id,
    core_subject_type,
    churned_from_date as last_lesson_before_churn,
    null            as return_date,
    days_since_last_lesson
from reactivations