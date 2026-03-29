{{ config(materialized='table') }}

WITH staging_loans AS (
    -- Kita tetep ngambil dari sumber utama yang udah dicuci
    SELECT * FROM {{ ref('stg_loans') }}
),

fct_loan_applications AS (
    SELECT
        -- 1. PRIMARY KEY & FOREIGN KEY
        application_id,              -- ID Struk Kasir (Transaksi)
        application_id AS client_id, -- Kunci buat nge-link ke buku biodata (dim_clients)
        
        -- 2. ATRIBUT TRANSAKSI
        contract_type,
        is_default,                  -- Ini Target Variable kita (0 = Lancar, 1 = Macet)

        -- 3. FACTS / MEASURES (Angka-angka kuantitatif)
        total_income_idr,
        loan_amount_idr,
        loan_annuity_idr

    FROM staging_loans
)

SELECT * FROM fct_loan_applications