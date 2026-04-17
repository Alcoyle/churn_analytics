{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

with completed as (
    select
        student_id,
        lesson_date,
        tutor_id,
        subject_id,
        core_subject_type
    from {{ ref('slv_lessons_enriched') }}
    where is_completed = true -- rmeoving the cancelled lessons as they did not take place.
),

with_previous as (
    select
        student_id,
        lesson_date,
        tutor_id,
        subject_id,
        core_subject_type,
        lag(lesson_date) over (
            partition by student_id
            order by lesson_date
        ) as previous_lesson_date    --window fucntion to get the last session date form the student in-situ
    from completed
),

with_gaps as (
    select
        *,
        date_diff(lesson_date, previous_lesson_date, day) as days_since_last_lesson
    from with_previous
)

select * from with_gaps