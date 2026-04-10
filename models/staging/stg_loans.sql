WITH source AS (
    SELECT * FROM {{ source('home_credit_raw', 'application_train') }}
),

renamed_and_casted AS (
    SELECT
        -- 1. Identifiers & Target
        CAST(sk_id_curr AS STRING) AS application_id,
        CAST(target AS INT64) AS is_default,
        CAST(name_contract_type AS STRING) AS contract_type,
        
        -- 2. Financial Metrics (Menggunakan NUMERIC untuk presisi uang)
        CAST(amt_income_total AS NUMERIC) AS total_income_idr,
        CAST(amt_credit AS NUMERIC) AS loan_amount_idr,
        CAST(amt_annuity AS NUMERIC) AS loan_annuity_idr,
        
        -- 3. Temporal Metrics (Tetap dipertahankan negatif sesuai raw, diubah di intermediate)
        CAST(days_birth AS INT64) AS days_birth,
        -- Mengubah anomali 365243 menjadi NULL agar tidak merusak agregasi rata-rata
        NULLIF(CAST(days_employed AS INT64), 365243) AS days_employed, 
        
        -- 4. Demographics & Dimension Attributes (Semua lowercase snake_case)
        CAST(code_gender AS STRING) AS gender,
        CAST(flag_own_car AS STRING) AS has_car_flag,
        CAST(flag_own_realty AS STRING) AS has_realty_flag,
        CAST(cnt_children AS INT64) AS total_children,
        CAST(name_income_type AS STRING) AS income_type,
        CAST(name_education_type AS STRING) AS education_type,
        CAST(name_family_status AS STRING) AS family_status,
        CAST(name_housing_type AS STRING) AS housing_type

    FROM source
)

-- Menghapus DISTINCT. Kita akan pasang test "unique" di file schema.yml untuk application_id
SELECT * FROM renamed_and_casted