WITH source AS (
    SELECT * FROM {{ source('home_credit_raw', 'application_train') }}
),

renamed_and_casted AS (
    SELECT
        -- Kolom ID dan Target
        CAST(SK_ID_CURR AS STRING) AS application_id,
        CAST(TARGET AS INT64) AS is_default,
        CAST(NAME_CONTRACT_TYPE AS STRING) AS contract_type,
        
        -- Kolom Keuangan
        CAST(AMT_INCOME_TOTAL AS FLOAT64) AS total_income_idr,
        CAST(AMT_CREDIT AS FLOAT64) AS loan_amount_idr,
        CAST(AMT_ANNUITY AS FLOAT64) AS loan_annuity_idr,
        
        -- Kolom Waktu
        CAST(DAYS_BIRTH AS INT64) AS days_birth,
        CAST(DAYS_EMPLOYED AS INT64) AS days_employed,

        -- 💡 TAMBAHAN BARU: Kolom Biodata buat Dimension Table
        CAST(CODE_GENDER AS STRING) AS CODE_GENDER,
        CAST(FLAG_OWN_CAR AS STRING) AS FLAG_OWN_CAR,
        CAST(FLAG_OWN_REALTY AS STRING) AS FLAG_OWN_REALTY,
        CAST(CNT_CHILDREN AS INT64) AS CNT_CHILDREN,
        CAST(NAME_INCOME_TYPE AS STRING) AS NAME_INCOME_TYPE,
        CAST(NAME_EDUCATION_TYPE AS STRING) AS NAME_EDUCATION_TYPE,
        CAST(NAME_FAMILY_STATUS AS STRING) AS NAME_FAMILY_STATUS,
        CAST(NAME_HOUSING_TYPE AS STRING) AS NAME_HOUSING_TYPE

    FROM source
)

SELECT DISTINCT * FROM renamed_and_casted