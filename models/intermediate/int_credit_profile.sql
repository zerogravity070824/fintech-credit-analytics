-- Materialization: table (dikonfigurasi di dbt_project.yml)

WITH loans AS (
    SELECT
        application_id,
        is_default,
        contract_type,
        total_income_idr,
        loan_amount_idr,
        loan_annuity_idr,
        days_birth,
        days_employed
    FROM {{ ref('stg_loans') }}
),

bureau_summary AS (
    SELECT
        application_id,
        COUNT(bureau_id)              AS total_previous_loans,
        SUM(total_bureau_debt_idr)    AS total_bureau_debt_idr
    FROM {{ ref('stg_bureau') }}
    GROUP BY application_id
),

joined_and_calculated AS (
    SELECT
        l.*,
        COALESCE(b.total_previous_loans, 0)   AS total_previous_loans,
        COALESCE(b.total_bureau_debt_idr, 0)  AS total_bureau_debt_idr,

        -- FIX: Dibagi pendapatan bulanan (tahunan / 12) agar DTI akurat
        {{ safe_divide('l.loan_annuity_idr', 'l.total_income_idr / 12') }} AS debt_to_income_ratio

    FROM loans l
    LEFT JOIN bureau_summary b
        ON l.application_id = b.application_id
),

joined_and_deduped AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY application_id) AS row_num
        FROM joined_and_calculated
    )
    WHERE row_num = 1
)

SELECT * FROM joined_and_deduped