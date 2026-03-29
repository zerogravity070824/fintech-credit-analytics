-- Tes ini nyari baris data yang jumlah pinjamannya 0 atau minus.
-- Kalau dbt nemu datanya, dbt bakal ngasih peringatan ERROR.

SELECT
    application_id,
    loan_amount_idr
FROM {{ ref('obt_credit_risk') }}
WHERE loan_amount_idr <= 0