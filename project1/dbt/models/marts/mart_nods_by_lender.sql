{{ config(
    materialized='table',
    cluster_by=['county_name']
) }}

select
    county_name,
    lender_name,
    count(*)                            as nod_count,
    round(avg(loan_amt), 2)             as avg_loan_amt,
    round(avg(ltv), 2)                  as avg_ltv,
    round(sum(loan_amt), 2)             as total_loan_amt
from {{ ref('stg_nods') }}
where lender_name is not null
    and lender_name != ''
group by 1, 2
