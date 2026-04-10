WITH source AS (
    SELECT * FROM {{ source('home_credit_raw', 'bureau') }}
),

renamed_and_casted AS (
    SELECT
        -- Primary Key & Foreign Key
        CAST(sk_id_curr AS STRING) AS application_id,
        CAST(sk_id_bureau AS STRING) AS bureau_id,
        
        -- Status kredit di bank lain
        CAST(credit_active AS STRING) AS credit_status,
        
        -- Waktu (harinya dibikin minus dari hari pengajuan)
        CAST(days_credit AS INT64) AS days_credit_before_application,
        
        -- Nominal uang (Wajib NUMERIC untuk data finansial)
        CAST(amt_credit_sum AS NUMERIC) AS total_bureau_credit_idr,
        CAST(amt_credit_sum_debt AS NUMERIC) AS total_bureau_debt_idr

    FROM source
)

-- Menggunakan QUALIFY untuk deduplikasi yang jauh lebih efisien di BigQuery
SELECT * FROM renamed_and_casted
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY bureau_id 
    ORDER BY days_credit_before_application DESC -- Mengambil data paling update jika ada duplikat
) = 1