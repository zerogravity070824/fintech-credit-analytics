{{ config(materialized='table') }}

WITH staging_loans AS (
    -- Kita ambil sumbernya dari staging yang udah bersih
    SELECT * FROM {{ ref('stg_loans') }}
),

dim_clients AS (
    -- Pake DISTINCT biar 1 nasabah cuma punya 1 baris biodata (nggak duplikat)
    SELECT DISTINCT
        application_id AS client_id, -- Ini jadi Primary Key buat buku biodata ini
        
        -- DI BAWAH INI ADALAH "KATA SIFAT" (Demografi Profil)
        CODE_GENDER AS gender,
        FLAG_OWN_CAR AS owns_car,
        FLAG_OWN_REALTY AS owns_realty,
        CNT_CHILDREN AS total_children,
        NAME_INCOME_TYPE AS income_type,
        NAME_EDUCATION_TYPE AS education_level,
        NAME_FAMILY_STATUS AS family_status,
        NAME_HOUSING_TYPE AS housing_type,
        
        -- Kita ubah umur yang minus ribuan hari jadi umur beneran
        CAST(ABS(days_birth) / 365 AS INT64) AS age_years,
        
        -- Kita ubah lama kerja jadi tahun juga
        CAST(ABS(days_employed) / 365 AS INT64) AS years_employed

    FROM staging_loans
)

SELECT * FROM dim_clients