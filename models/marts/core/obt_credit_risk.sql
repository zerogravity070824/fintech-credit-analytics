{{ config(materialized='table') }}

WITH fact_loans AS (
    -- Ambil struk kasir
    SELECT * FROM {{ ref('fct_loan_applications') }}
),

dim_clients AS (
    -- Ambil buku biodata
    SELECT * FROM {{ ref('dim_clients') }}
),

one_big_table AS (
    SELECT
        -- 1. Ambil semua angka dan ID dari struk kasir (fact_loans)
        --- 2. * artinya semua kolom. Isinya: ID Transaksi, Jumlah Pinjaman, Status Macet (is_default), dll.
        f.*,
        
        -- 2. Tempelin "Kata Sifat" dari buku biodata (dim_clients)
        -- 3. c. itu singkatan dari dim_clients.
        -- (Kita sebutin satu-satu biar rapi dan client_id gak dobel)
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
    LEFT JOIN dim_clients c
        ON f.client_id = c.client_id
)

SELECT * FROM one_big_table