WITH source AS (
    -- Ngambil data histori kredit dari bank lain
    SELECT * FROM {{ source('home_credit_raw', 'bureau') }}
),

renamed_and_casted AS (
    SELECT
        -- Primary Key & Foreign Key
        CAST(SK_ID_CURR AS STRING) AS application_id,
        CAST(SK_ID_BUREAU AS STRING) AS bureau_id,
        
        -- Status kredit di bank lain (misal: Active, Closed)
        CAST(CREDIT_ACTIVE AS STRING) AS credit_status,
        
        -- Waktu (harinya dibikin minus dari hari pengajuan)
        CAST(DAYS_CREDIT AS INT64) AS days_credit_before_application,
        
        -- Nominal uang
        CAST(AMT_CREDIT_SUM AS FLOAT64) AS total_bureau_credit_idr,
        CAST(AMT_CREDIT_SUM_DEBT AS FLOAT64) AS total_bureau_debt_idr

    FROM source
),

deduplicated AS (
    -- Ngebuang data duplikat biar aman pas di-join nanti
    SELECT DISTINCT * FROM renamed_and_casted
)

SELECT * FROM deduplicated