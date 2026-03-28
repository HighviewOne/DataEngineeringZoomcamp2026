{{ config(
    materialized='table',
    partition_by={
        'field': 'week_start',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['county_name']
) }}

select
    date_trunc(recording_date, week)    as week_start,
    county_name,
    count(*)                            as nod_count,
    round(avg(loan_amt), 2)             as avg_loan_amt,
    round(avg(ltv), 2)                  as avg_ltv,
    round(avg(min_bid), 2)              as avg_min_bid
from {{ ref('stg_nods') }}
where recording_date is not null
group by 1, 2
