{{ config(materialized='table') }}

WITH fact_loans AS (
    SELECT * FROM {{ ref('fct_loan_applications') }}
),

dim_clients AS (
    SELECT * FROM {{ ref('dim_clients') }}
),

one_big_table AS (
    SELECT
        -- Keys & Target
        f.application_id,
        f.is_default,
        f.contract_type,

        -- Financial Measures
        f.total_income_idr,
        f.loan_amount_idr,
        f.loan_annuity_idr,

        -- FIX: Risk Metrics dari int_credit_profile (via fct)
        f.debt_to_income_ratio,
        f.total_previous_loans,
        f.total_bureau_debt_idr,

        -- Demographic Attributes dari dim_clients
        c.gender,
        c.owns_car,
        c.owns_realty,
        c.total_children,
        c.income_type,
        c.education_level,
        c.family_status,
        c.housing_type,
        c.age_years,
        c.years_employed

    FROM fact_loans f
    LEFT JOIN dim_clients c ON f.application_id = c.client_id
)

SELECT * FROM one_big_table