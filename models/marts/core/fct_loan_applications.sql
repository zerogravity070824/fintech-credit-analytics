{{ config(materialized='table') }}

WITH int_profile AS (
    SELECT * FROM {{ ref('int_credit_profile') }}  -- FIX: dari int bukan stg
),

fct_loan_applications AS (
    SELECT
        application_id,
        contract_type,
        is_default,
        total_income_idr,
        loan_amount_idr,
        loan_annuity_idr,
        -- FIX: tambahkan kolom penting dari int_credit_profile
        debt_to_income_ratio,
        total_previous_loans,
        total_bureau_debt_idr
    FROM int_profile
)

SELECT * FROM fct_loan_applications