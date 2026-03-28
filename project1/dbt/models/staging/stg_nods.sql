{{ config(materialized='view') }}

with source as (
    select * from {{ source('nod_raw', 'nods') }}
),

cleaned as (
    select
        -- identifiers
        APN                                                     as apn,
        document_number,
        county,

        -- county label
        case coalesce(county, '')
            when ''   then 'Los Angeles'
            when 'OC' then 'Orange'
            when 'RI' then 'Riverside'
            when 'SD' then 'San Diego'
            when 'SR' then 'San Bernardino'
            when 'VE' then 'Ventura'
            else county
        end                                                     as county_name,

        -- property location
        Situs_House                                             as property_house,
        Situs_Street                                            as property_street,
        Situs_City                                              as property_city,
        Situs_Zip                                               as property_zip,
        zoning,
        use_code,
        safe_cast(yr_built as int64)                            as yr_built,
        safe_cast(bed as int64)                                 as bedrooms,
        safe_cast(bath as float64)                              as bathrooms,
        safe_cast(sq_feet as int64)                             as sq_feet,

        -- owner
        trustor_full_name                                       as owner_name,

        -- financial
        ltv,
        loan_amt,
        min_bid,
        safe_cast(assessed_value as float64)                    as assessed_value,

        -- lender
        beneficiary_name                                        as lender_name,

        -- trustee
        trustee_name,

        -- dates (parse MM/DD/YY or MM/DD/YYYY)
        safe.parse_date('%m/%d/%y',  recording_date)            as recording_date,
        safe.parse_date('%m/%d/%Y',  recording_date)            as recording_date_4y,
        safe.parse_date('%m/%d/%y',  sale_date)                 as sale_date,
        safe.parse_date('%m/%d/%Y',  sale_date)                 as sale_date_4y,
        safe.parse_date('%m/%d/%y',  loan_date)                 as loan_date,

        -- lat/lon
        latitude,
        longtitude                                              as longitude

    from source
    where APN is not null
        and APN != 'APN'  -- skip header rows if any
)

select
    apn,
    document_number,
    county,
    county_name,
    property_house,
    property_street,
    property_city,
    property_zip,
    zoning,
    use_code,
    yr_built,
    bedrooms,
    bathrooms,
    sq_feet,
    owner_name,
    ltv,
    loan_amt,
    min_bid,
    assessed_value,
    lender_name,
    trustee_name,
    coalesce(recording_date, recording_date_4y)     as recording_date,
    coalesce(sale_date, sale_date_4y)               as sale_date,
    loan_date,
    latitude,
    longitude
from cleaned
