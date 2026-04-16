{{ config(materialized='table') }}

WITH staging_loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

dim_clients AS (
    SELECT
        application_id AS client_id,
        gender,
        has_car_flag AS owns_car,
        has_realty_flag AS owns_realty,
        total_children,
        income_type,
        education_type AS education_level,
        family_status,
        housing_type,
        -- Aman dari anomali 365243 karena sudah di-handle di stg_loans
        CAST(ABS(days_birth) / 365 AS INT64) AS age_years,
        CAST(ABS(days_employed) / 365 AS INT64) AS years_employed
    FROM staging_loans
    -- FIX: QUALIFY lebih andal dari DISTINCT untuk deduplikasi PK
    QUALIFY ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY application_id) = 1
)

SELECT * FROM dim_clients