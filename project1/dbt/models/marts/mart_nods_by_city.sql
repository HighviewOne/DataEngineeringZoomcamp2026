{{ config(
    materialized='table',
    cluster_by=['county_name']
) }}

select
    county_name,
    property_city,
    count(*)                            as nod_count,
    round(avg(loan_amt), 2)             as avg_loan_amt,
    round(avg(ltv), 2)                  as avg_ltv,
    round(avg(min_bid), 2)              as avg_min_bid,
    min(recording_date)                 as first_nod_date,
    max(recording_date)                 as last_nod_date
from {{ ref('stg_nods') }}
where property_city is not null
    and property_city != ''
group by 1, 2
