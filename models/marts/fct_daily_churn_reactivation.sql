{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

with events as (
    select * from {{ ref('slv_churn_reactivation_events') }}
),

pivoted as (
    select
        event_date,
        countif(event_type = 'churn')        as daily_churns,
        countif(event_type = 'reactivation') as daily_reactivations
    from events
    where event_date >= '2026-02-01'
      and event_date <  '2026-03-01'
    group by 1
)

select
    event_date,
    daily_churns,
    daily_reactivations,
    sum(daily_churns)        over (order by event_date) as cumulative_churns,
    sum(daily_reactivations) over (order by event_date) as cumulative_reactivations
from pivoted
order by event_date