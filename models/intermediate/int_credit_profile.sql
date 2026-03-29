{{ config(materialized='table') }}

WITH loans AS (
    -- Mengambil data dari layer staging pake fungsi ref()
    SELECT * FROM {{ ref('stg_loans') }}
),

bureau_summary AS (
    -- Karena 1 orang bisa punya banyak histori kredit, kita grup (agregasi) dulu per application_id
    SELECT 
        application_id,
        COUNT(bureau_id) AS total_previous_loans,
        SUM(total_bureau_debt_idr) AS total_bureau_debt_idr
    FROM {{ ref('stg_bureau') }}
    GROUP BY application_id
),

joined_and_calculated AS (
    SELECT 
        l.*,
        -- Pake COALESCE biar kalo dia gak punya histori kredit, nilainya jadi 0 (bukan null)
        COALESCE(b.total_previous_loans, 0) AS total_previous_loans,
        COALESCE(b.total_bureau_debt_idr, 0) AS total_bureau_debt_idr,
        
        -- LOGIKA BISNIS: Menghitung Debt-to-Income Ratio (DTI)
        -- Cicilan bulanan dibagi total pendapatan setahun
        SAFE_DIVIDE(l.loan_annuity_idr, l.total_income_idr) AS debt_to_income_ratio

    FROM loans l
    LEFT JOIN bureau_summary b 
        ON l.application_id = b.application_id
)

SELECT * FROM joined_and_calculated